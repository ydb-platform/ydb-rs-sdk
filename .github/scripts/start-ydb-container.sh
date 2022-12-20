#!/bin/bash

docker run -d --rm --name ydb-local -h localhost -p 2136:2136 -e YDB_USE_IN_MEMORY_PDISKS=false amyasnikov/ydb:slim

while ! docker run --network host amyasnikov/ydb:slim /ydb -e grpc://localhost:2136 -d /local scheme ls; do
  echo wait db...
  sleep 3
done
echo DB available
