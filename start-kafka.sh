#!/bin/bash

set -eu

docker-compose up -d

n=0
while ! docker exec wolff-kafka bash -c '/opt/kafka/bin/kafka-topics.sh --zookeeper zookeeper --list' | grep 'test-topic'; do
  if [ $n -gt 10 ]; then
    echo "timeout waiting for kafka"
    exit 1
  fi
  n=$(( n + 1 ))
  sleep 1
done
