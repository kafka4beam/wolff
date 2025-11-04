#!/bin/bash -eu

if [ -n "${DEBUG:-}" ]; then
    set -x
fi

TD="$(cd "$(dirname "$0")" && pwd)"

. "$TD/.env"

docker ps > /dev/null || {
    echo "You must be a member of docker group to run this script"
    exit 1
}

function docker_compose() {
    if command -v docker-compose ; then
        docker-compose $@
    else
        docker compose version &> /dev/null
        if [ $? -eq 0 ]; then
            docker compose $@
        else
            exit "couldn't find docker compose, needed for testing"
        fi
    fi
}

KAFKA_VERSION="${KAFKA_VERSION:-${1:-4.0.0}}"

case $KAFKA_VERSION in
  0.9*)
    KAFKA_VERSION="0.9"
    ;;
  0.10*)
    KAFKA_VERSION="0.10"
    ;;
  0.11*)
    KAFKA_VERSION="0.11"
    ;;
  1.*)
    KAFKA_VERSION="1.1"
    ;;
  2.*)
    KAFKA_VERSION="2.8"
    ;;
  3.*)
    KAFKA_VERSION="3.9"
    ;;
  4.*)
    KAFKA_VERSION="4.0"
    ;;
  *)
    echo "Unsupported version $KAFKA_VERSION"
    exit 1
    ;;
esac

KAFKA_IMAGE_VERSION="${KAFKA_IMAGE_VERSION:-1.2.1}"
export KAFKA_IMAGE_TAG="zmstone/kafka:${KAFKA_IMAGE_VERSION}-${KAFKA_VERSION}"
echo "Using $KAFKA_IMAGE_TAG"

KAFKA_MAJOR=$(echo "$KAFKA_VERSION" | cut -d. -f1)
if [ "$KAFKA_MAJOR" -lt 3 ]; then
    NEED_ZOOKEEPER=true
else
    NEED_ZOOKEEPER=false
fi

function bootstrap_opts() {
  if [[ "$NEED_ZOOKEEPER" = true ]]; then
    echo "--zookeeper ${ZOOKEEPER_IP}:2181"
  else
    echo "--bootstrap-server ${KAFKA_1_IP}:9092"
  fi
}

docker_compose -f $TD/docker-compose.yml down || true
docker_compose -f $TD/docker-compose-kraft.yml down || true

if [[ "$NEED_ZOOKEEPER" = true ]]; then
  docker_compose -f $TD/docker-compose.yml up -d
else
  docker_compose -f $TD/docker-compose-kraft.yml up -d
fi

# give kafka some time
sleep 5

MAX_WAIT_SEC=10

function wait_for_kafka() {
  local which_kafka="$1"
  local n=0
  local topic_list listener
  while true; do
    cmd="opt/kafka/bin/kafka-topics.sh $(bootstrap_opts) --list"
    topic_list="$(docker exec $which_kafka $cmd 2>&1)"
    if [ "${topic_list-}" = '' ]; then
      break
    fi
    if [ $n -gt $MAX_WAIT_SEC ]; then
      echo "timeout waiting for $which_kafka"
      echo "last print: ${topic_list:-}"
      exit 1
    fi
    n=$(( n + 1 ))
    sleep 1
  done
}

wait_for_kafka kafka-1
wait_for_kafka kafka-2

function create_topic() {
  TOPIC_NAME="$1"
  PARTITIONS="${2:-1}"
  REPLICAS="${3:-1}"
  CMD="/opt/kafka/bin/kafka-topics.sh $(bootstrap_opts) --create --partitions $PARTITIONS --replication-factor $REPLICAS --topic $TOPIC_NAME --config min.insync.replicas=1"
  docker exec kafka-1 bash -c "$CMD"
}

create_topic "dummy" || true
create_topic "test-topic"
create_topic "test-topic-2" 2 1
create_topic "test-topic-3" 1 1
