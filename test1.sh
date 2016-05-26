#!/usr/bin/env bash

for (( i=50; i<=1000; i=i+50))
do
for j in {1..3}
do
python3 service_test.py > /dev/null 2>&1 &
sleep 2
if [ $2 == "2" ]; then
curl http://127.0.0.1:33600/$1?type=0
fi
siege http://127.0.0.1:33600/$1?type=$2 -c ${i} -r 20 2>>./log/$1_4_${i}_20.log
sleep 5
kill $(ps -ef | grep service_test | awk '{print $2}')
sleep 3
done
done
