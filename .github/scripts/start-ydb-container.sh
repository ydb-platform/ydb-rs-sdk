#!/bin/bash

docker run -d --rm --name ydb-local -h localhost -p 2136:2136 -e YDB_USE_IN_MEMORY_PDISKS=false amyasnikov/ydb:latest

while ! docker run --network host ydb-local /ydb -e grpc://localhost:2136 -d /local scheme ls; do
  echo wait db...
  sleep 3
done
echo DB available
