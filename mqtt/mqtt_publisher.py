#########################################################
#
# Examples:
#
#   Parameter examples:
#
#    --broker localhost --port 1884 --clientid py-pub-01 --qos 1 --nummsgs 5000 --delay 0 --topic sometopic
#
#
#    Linux Message:
#       --message "{\"field\":\"blah\"}"
	
#    Windows message:
#       --message "{\""value\"":\""blah\""}"
#
#   Linux:
#
#     python3 mqtt_publisher.py <params>
#
#
#   Windows Terminal:
#
#     py .\mqtt_publisher.py <params>
#     or use "python"
#
# Help:
#
#   python3 mqtt_publisher.py -h
#
#########################################################

import sys
import paho.mqtt.client as mqtt  #import the subscribing_client
import time
import logging,sys
import argparse
import datetime


#broker="iot.eclipse.org"
#broker="broker.hivemq.com"
keepalive=1200

# parse args
parser=argparse.ArgumentParser()
parser.add_argument('--broker', help='MQTT Broker URL or IP')
parser.add_argument('--port', help='MQTT Broker Port')
parser.add_argument('--clientid', help='')
parser.add_argument('--qos', help='')
parser.add_argument('--nummsgs', help='')
parser.add_argument('--delay', help='Delay between publishing messages in seconds')
parser.add_argument('--cleansession', help='')
parser.add_argument('--topic', help='')
parser.add_argument('--message', help='Custom message to send to topic')
args=parser.parse_args()

logging.basicConfig(level=logging.DEBUG)
#use DEBUG,INFO,WARNING

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

print(args)

def on_disconnect(client, userdata, flags, rc=0):
    m="DisConnected flags"+"result code: " + str(rc) + ", subscribing_client_id: " + str(client)
    print(m)

def on_connect(client, userdata, flags, rc):
    m="Connected flags"+str(flags)+"result code: " + str(rc) + ", subscribing_client_id: " + str(client)
    print(m)

# Called when a message that was to be sent using the publish() call has completed transmission to the broker.
# For messages with QoS levels 1 and 2, this means that the appropriate handshakes have completed.
# For QoS 0, this simply means that the message has left the client.
def on_publish(client, userdata, mid):
    m="Broker ack received, result code: " + str(userdata) + "; subscribing_client_id: " + str(mid)
    print(m)
    global pub_ack
    pub_ack=True
    pass

def pub(client,topic,msg,qos,p_msg):
    logging.info(datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S") + " " + p_msg + " publishing " + msg + " to topic="+topic +" with qos="+str(qos))
    ret=client.publish(topic, msg, qos)
    logging.info('Publish result: ' + str(ret))

publishing_client = mqtt.Client(args.clientid)    #create new instance

# attache callback functions
publishing_client.on_connect=on_connect
publishing_client.on_publish=on_publish
publishing_client.on_disconnect=on_disconnect

logging.info("Connecting...")
publishing_client.connect(args.broker, int(args.port), keepalive)      #connect to broker
# run a thread in background to handle the network connection and sending/receiving data
publishing_client.loop_start()

print("Publishing +" + str(int(args.nummsgs)) + " messages...")

for x in range(1, int(args.nummsgs)+1):
  pub_ack=False

  if not args.message:
    message="Message "+str(x)
  else:
    message=args.message
    
  time.sleep(float(args.delay)) # siumlate speed of client (3/sec)
  
  pub(publishing_client, args.topic, message, int(args.qos), args.clientid)
  while pub_ack != True:
    #time.sleep(.01) # takes a non-zero amount of time to get the ACK back for QoS > 0 (on_publish called)
    pass

publishing_client.disconnect() # disconnect from broker
publishing_client.loop_stop()
print("Done")
