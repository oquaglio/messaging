# Invocation
#
# Powershell:
# py .\mqtt_publisher.py <params>
#
# Params:
# --host localhost --port 5432 --database=integration --table=table_name --user <username> --password <password> --delay 100 --numrecords 1000 --silent 0
#
# Purpose
#
# Insert records continuosly to provide test data for integration
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
parser.add_argument('--numrecords', help='Number of records to insert')
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

# insert new records
i = 1
while i <= int(args.numrecords):
    dt = datetime.datetime.now(pytz.timezone('Australia/Perth'))
    if not int(args.silent):
        print("INSERT INTO " + args.table + " (created, updated, initial_value, current_value) VALUES ('%s', '%s', '%d', '%d')" %(dt, dt, i, i))
    cursor.execute("INSERT INTO " + args.table + " (created, updated, initial_value, current_value) VALUES ('%s', '%s', '%d', '%d')" %(dt, dt, i, i))
    #cursor.execute("INSERT INTO " + args.table + " (created, updated, initial_value, current_value) VALUES (%s, %s, %d, %d)", (dt,dt,%1,%1))
    conn.commit()
    i += 1
    time.sleep(float(args.delay))

conn.close()