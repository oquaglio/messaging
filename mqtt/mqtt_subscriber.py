#########################################################
#
# Run with: python3 test-subscriber.py <params>
#
# NOTES: 
#    - I had weird issues using ipaddress of local docker containers. Seems to work okay using localhost:port. This seemed to 
#      impact subscriber only.
#    - If you subscribe to a particular topic, any messages waiting for the client id on previously subscribed topics will also
#      be delivered
# 
#
# Examples:
#   python3 mqtt_subscriber.py --broker localhost --port 1884 --clientid py-sub-01 --qos 1 --cleansession false --topic test/topic
#   
# Windows Terminal:
#   py ./mqtt_subscriber.py --broker localhost --port 1884 --clientid py-sub-01 --qos 1 --cleansession false --topic test/in
#   
#
# For help:
#   python3 mqtt_subscriber.py -h 
#     
#    
#########################################################

import sys
import paho.mqtt.client as mqtt  #import the subscribing_client
import time
import logging,sys
import argparse

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')    


# parse args
parser=argparse.ArgumentParser()
parser.add_argument('--broker', help='MQTT Broker URL or IP')
parser.add_argument('--port', help='MQTT Broker Port')
parser.add_argument('--clientid', help='')
parser.add_argument('--qos', help='')
parser.add_argument('--cleansession', help='')
parser.add_argument('--topic', help='')
args=parser.parse_args()

print(args)

keepalive=1200
r_messages=[]

logging.basicConfig(level=logging.DEBUG)
#use DEBUG,INFO,WARNING
def on_disconnect(client, userdata, flags, rc=0):
    m="DisConnected flags"+"result code "+str(rc)+"subscribing_client_id  "+str(client)
    print(m)

def on_connect(client, userdata, flags, rc):
    m="Connected flags"+str(flags)+"result code "+str(rc)+"subscribing_client_id  "+str(client)    
    print(m)

def on_message(subscribing_client, userdata, message):
    msg=str(message.payload.decode("utf-8"))
    #logging.info('Received message "' +msg +'" on topic "' + message.topic + '"')
    m='Received message "' +msg +'" on topic "' + message.topic + '"'
    print(m)
    r_messages.append(msg)

def sub(client,topic,qos,s_msg):
    m=s_msg+" subscribing to topic="+topic +" with qos="+str(qos)
    logging.info(m)
    #print(m)	
    client.subscribe(topic,qos)       

print(sys.argv[1] + " " + sys.argv[2] + " " + sys.argv[3] + " " + sys.argv[4] + " " + sys.argv[5] + " " + sys.argv[6])

subscribing_client = mqtt.Client(args.clientid, clean_session=str2bool(args.cleansession))    #create new instance

# attache callback functions
subscribing_client.on_message=on_message        
subscribing_client.on_connect=on_connect               
subscribing_client.on_disconnect=on_disconnect                

print("Connecting with CLEAN_SESSION=",args.cleansession)
subscribing_client.connect(args.broker,int(args.port),keepalive)
subscribing_client.loop_start()
sub(subscribing_client, args.topic, int(args.qos), args.clientid)
#time.sleep(3)                      # subscribe for x seconds then stop, or...
inp=input("Waiting to continue:")   # press a key to stop subscribing

print("Received " + str(len(r_messages)) + " message(s)");

subscribing_client.loop_stop(); # stop checking buffer for inbound messages
subscribing_client.disconnect() # disconnect from broker
print("Done")
