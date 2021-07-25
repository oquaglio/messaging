#########################################################
#
# Run with: python3 test-subscriber.py <params>
#
# NOTE: I had weird issues using ipaddress of local docker containers. Seems to work okay using localhost:port. This seemed to impact subscriber only.
#
# Examples:
#   python3 mqtt_subscriber.py --broker localhsot --port 1883 --clientid py-sub-01 --qos 1 --nummsgs 100 --cleansession false --topic test/topic2
#   
# Windows Terminal:
#   py ./mqtt_subscriber.py --broker localhost --port 1884 --clientid py-sub-01 --qos 1 --nummsgs 100 --cleansession false --topic test/in
#  
#
# For help:
#   python3 mqtt_subscriber.py -h 
#     
#    
#########################################################
#!/usr/bin/python

import sys
import paho.mqtt.client as mqtt  #import the client
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
parser.add_argument('--nummsgs', help='[Optional] Wait until n messages received then exit [default is to wait for any key]')
parser.add_argument('--cleansession', help='')
parser.add_argument('--topic', help='')
args=parser.parse_args()

print(args)

output_stream = sys.stdout
keepalive=1200
r_messages=[]
message_count=0
bytes_received=0
connected_Flag=False
#global connected_Flag
#connected_Flag=False

logging.basicConfig(level=logging.DEBUG)
#use DEBUG,INFO,WARNING
def on_disconnect(client, userdata, flags, rc=0):
    m="DisConnected flags"+"result code "+str(rc)+"client_id  "+str(client)
    #print(m)
    print ("Disconnected")    

def on_connect(client, userdata, flags, rc):
    m="Connected: flags"+str(flags)+"result code: "+str(rc)+"client_id: "+str(client)    
    #print(m)
#    print ("Connected")    
    global connected_Flag

    if rc == 0:
        connected_Flag=True #set flag
        print("connected OK, return code: ",rc)
    else:
        print("Bad connection, return code: ",rc)

def on_message(client, userdata, message):
    global message_count, bytes_received
    #msg = str(message.payload.decode("utf-8"))
    #bytes_received += len(msg.encode('utf-8'))
    bytes_received += len(message.payload)
    #logging.info('Received message "' +msg +'" on topic "' + message.topic + '"')
    #m='Received message "' +msg +'" on topic "' + message.topic + '"'
    #print(m)
    #r_messages.append(msg)
    message_count += 1
    output_stream.write('Message(s) received: %s\r' % message_count)
    output_stream.flush()

def on_log(client, userdata, level, buf):
    m = buf
    #print("log: ",m)      

#print(sys.argv[1] + " " + sys.argv[2] + " " + sys.argv[3] + " " + sys.argv[4] + " " + sys.argv[5] + " " + sys.argv[6])

client = mqtt.Client(args.clientid, clean_session=str2bool(args.cleansession))    #create new instance

client.subscribe(args.topic, int(args.qos))

# attach callback functions
client.on_message=on_message        
client.on_connect=on_connect               
client.on_disconnect=on_disconnect                
client.on_log=on_log

#print("Connecting with CLEAN_SESSION=",args.cleansession)

try:
    client.connect(args.broker,int(args.port),keepalive) # blocks until connected
except:
    print("connection failed")
    exit(1) #Should quit or raise flag to quit or retry


#print(connected_Flag)
#while connected_Flag!=True: #wait in loop
#   time.sleep(0.1)


client.loop_start()

#client.loop_forever()

#time.sleep(3)                      # subscribe for x seconds then stop, or...
inp=input("")   # press a key to stop subscribing

#print("Received " + str(len(r_messages)) + " message(s)");
#print("Received " + str(message_count) + " message(s)");
print("Byte(s) received: " + str(bytes_received));
print("Avg byte(s)/message: " + str(bytes_received/message_count));

#time.sleep(3)
client.loop_stop(); # stop checking buffer for inbound messages
client.disconnect() # disconnect from broker
#print("Done")
