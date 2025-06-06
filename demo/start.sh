#!/bin/bash
trap "rm server;kill 0" EXIT

go build -o server -gcflags="all=-N -l"
./server -port=8001 &
./server -port=8002 &
./server -port=8003 -api=1 &

sleep 2
echo ">>> start test"
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &

curl "http://localhost:9999/api?key=Sam" &
curl "http://localhost:9999/api?key=Jack" &
curl "http://localhost:9999/api?key=Amy" &

curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Sam" &
curl "http://localhost:9999/api?key=Jack" &
curl "http://localhost:9999/api?key=Amy" &

wait