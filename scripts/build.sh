#!/usr/bin/env bash
printf "\n\n######## kafka-streams aggregator build ########\n"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR/..

IMAGE_REPOSITORY=${IMAGE_REPOSITORY:-quay.io/redhatdemo/2021-kafka-streams-match-aggregator:latest}

./mvnw package
docker build $DIR/../aggregator -f src/main/docker/Dockerfile.jvm -t $IMAGE_REPOSITORY
