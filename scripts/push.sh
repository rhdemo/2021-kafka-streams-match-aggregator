#!/usr/bin/env bash
printf "\n\n######## kafka-streams aggregator push ########\n"

IMAGE_REPOSITORY=${IMAGE_REPOSITORY:-quay.io/redhatdemo/2021-kafka-streams-match-aggregator:latest}

echo "Pushing ${IMAGE_REPOSITORY}"
docker push ${IMAGE_REPOSITORY}
