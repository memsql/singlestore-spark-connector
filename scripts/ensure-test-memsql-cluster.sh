#!/usr/bin/env bash
set -eu

CONTAINER_NAME="memsql-spark-utils-test"

EXISTS=$(docker inspect ${CONTAINER_NAME} >/dev/null 2>&1 && echo 1 || echo 0)

if [[ "${EXISTS}" -eq 0 ]]; then
    docker run -i --init \
        --name ${CONTAINER_NAME} \
        -e LICENSE_KEY=${LICENSE_KEY} \
        -p 5506:3306 -p 5507:3307 \
        memsql/cluster-in-a-box
fi

docker start ${CONTAINER_NAME}
