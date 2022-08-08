#!/bin/bash

docker run -d --rm --name ydb-local -h localhost -p 2136:2136 -e YDB_USE_IN_MEMORY_PDISKS=false cr.yandex/yc/yandex-docker-local-ydb:latest

while ! docker run --network host cr.yandex/yc/yandex-docker-local-ydb:latest /ydb -e grpc://localhost:2136 -d /local scheme ls; do
  echo wait db...
  sleep 3
done
echo DB available
