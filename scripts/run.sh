#!/usr/bin/env bash

IMAGE_REPOSITORY=${IMAGE_REPOSITORY:-quay.io/redhatdemo/2021-kafka-streams-match-aggregator:latest}

docker run \
--rm \
-p 8080:8080 \
--name kafka-streams-match-aggregator \
-e LOG_LEVEL=TRACE \
-e KAFKA_SVC_USERNAME=$KAFKA_SVC_USERNAME \
-e KAFKA_SVC_PASSWORD=$KAFKA_SVC_PASSWORD \
-e KAFKA_BOOTSTRAP_URL=$KAFKA_BOOTSTRAP_URL \
$IMAGE_REPOSITORY
