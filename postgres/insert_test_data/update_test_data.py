# Invocation
#
# Powershell:
# py .\mqtt_publisher.py <params>
#
# Params:
# --host localhost --port 5432 --database=integration --table=table_name --user <username> --password <password> --delay 100 --numrecords 10 --silent 0 --numupdates 1000
#
# numrecords = number of records to update at any one time
#
# Purpose
#
# Update records continuosly to provide test data for integration
#
################################################################################

import time
import logging,sys
import argparse
import datetime,pytz
import random
import string
import psycopg2


parser=argparse.ArgumentParser()
parser.add_argument('--host', help='Hostname')
parser.add_argument('--port', help='Postgres Port')
parser.add_argument('--database', help='Postgres database name')
parser.add_argument('--table', help='Postgres table name')
parser.add_argument('--user', help='')
parser.add_argument('--password', help='')
parser.add_argument('--delay', help='Delay between inserting new records in millis')
parser.add_argument('--numrecords', help='Number of records to update per update')
parser.add_argument('--numupdates', help='Number of records to update per update')
parser.add_argument('--silent', help='Logging? 1 suppress logging, 0 logging')
args=parser.parse_args()

logging.basicConfig(level=logging.DEBUG)
#use DEBUG,INFO,WARNING

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

print(args)

# connect
conn = psycopg2.connect( database=args.database, user=args.user, password=args.password, host=args.host, port=args.port)
conn.autocommit = True
cursor = conn.cursor()

# select records to update
select_query = "select id, created, initial_value, updated, current_value from " + args.table +  \
                " order by updated, id asc \
                FETCH NEXT %d ROWS ONLY" %int(args.numrecords)

# insert new records
i = 1
while i <= int(args.numupdates):
    dt = datetime.datetime.now(pytz.timezone('Australia/Perth'))
    if not int(args.silent):
        print(select_query)
    cursor.execute(select_query)
    result = cursor.fetchall()
    for record in result:
        #print(record)
        #print("id: ", str(record[0]))
        #print("created: ", str(record[1]) + " Type: " + str(type(record[1])))
        #print("initial_value: " + str(record[2]))
        #print("updated: " + str(record[3]))
        #print("current_value: " + str(record[4]))
        #created = record[1]
        #print("created: " + str(created))
        update_string = "update " + args.table + " set updated = '%s', current_value=%d where id = %d" %(dt, record[4]+1, record[0])
        if not int(args.silent):
            print(update_string)
        cursor.execute(update_string)
        #cursor.execute("INSERT INTO " + args.table + " (created, updated, initial_value, current_value) VALUES ('%s', '%s', '%d', '%d')" %(dt, dt, i, i))
        #cursor.execute("INSERT INTO " + args.table + " (created, updated, initial_value, current_value) VALUES (%s, %s, %d, %d)", (dt,dt,%1,%1))
        conn.commit()
    i += 1
    time.sleep(float(args.delay))

conn.close()