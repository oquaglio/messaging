#!/bin/bash
# Usage ./mqtt_load_test.sh [#clients] [#messages per client] [message size in bytes]

echo "Spawning $1 subscribers"

time for (( i=1; i <= $1; i++ )); do      
  time python3 mqtt_subscriber.py --broker 127.17.0.5 --port 1885 --clientid subscriber-$i --qos 1 --nummsgs $2 --cleansession false --topic test/topic2 &
done


