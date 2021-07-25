#!/bin/bash
# Usage ./mqtt_load_test.sh <broker> <port> <topic> [#clients] [#messages per client] [message size in bytes]
# Examples:
# ./mqtt_multi_publisher_load_test.sh localhost 1886 testtopic 10 1000 2000

echo "Spawning $4 clients to publish $5 message(s) of size $6 byte(s) each to broker $1:$2 on topic $3"
message=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w $6 | head -n 1)
#for i in {0..$1}

time for (( i=1; i <= $4; i++ )); do # spin up n clients...
#do
  #echo "python3 mqtt_publisher.py --broker 127.17.0.5 --port 1885 --clientid py-pub-$i --qos 1 --nummsgs $5 --topic test/topic2 --message..."
  echo "python3 mqtt_publisher.py --broker $1 --port $2 --clientid py-pub-$i --qos 1 --nummsgs $5 --topic $3 --delay 0 --message [omitted]"
  
  #python3 mqtt_publisher.py --broker 127.17.0.5 --port 1885 --clientid publisher-$i --qos 1 --nummsgs $4 --topic test/topic2 --message $message &> /dev/null &
  python3 mqtt_publisher.py --broker $1 --port $2 --clientid publisher-$i --qos 1 --nummsgs $5 --topic $3 --delay 0 --message $message &> /dev/null &
done

printf "Messages published: %d\n" "$(($4 * $5))"
