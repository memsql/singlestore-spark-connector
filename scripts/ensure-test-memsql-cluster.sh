#!/usr/bin/env bash
set -eu

DEFAULT_IMAGE_NAME="memsql/cluster-in-a-box:centos-7.0.11-df50c6ab30-1.9.5-1.4.1"
IMAGE_NAME="${MEMSQL_IMAGE:-$DEFAULT_IMAGE_NAME}"
CONTAINER_NAME="memsql-spark-utils-test"

EXISTS=$(docker inspect ${CONTAINER_NAME} >/dev/null 2>&1 && echo 1 || echo 0)

if [[ "${EXISTS}" -eq 1 ]]; then
  EXISTING_IMAGE_NAME=$(docker inspect -f '{{.Config.Image}}' ${CONTAINER_NAME})
  if [[ "${IMAGE_NAME}" != "${EXISTING_IMAGE_NAME}" ]]; then
    echo "Existing container ${CONTAINER_NAME} has image ${EXISTING_IMAGE_NAME} when ${IMAGE_NAME} is expected; recreating container."
    docker rm -f ${CONTAINER_NAME}
    EXISTS=0
  fi
fi

if [[ "${EXISTS}" -eq 0 ]]; then
    docker run -i --init \
        --name ${CONTAINER_NAME} \
        -e LICENSE_KEY=${LICENSE_KEY} \
        -p 5506:3306 -p 5507:3307 \
        ${IMAGE_NAME}
fi

docker start ${CONTAINER_NAME}

echo -n "Waiting for MemSQL to start..."
while true; do
    if mysql -u root -h 127.0.0.1 -P 5506 -e "select 1" >/dev/null 2>/dev/null; then
        break
    fi
    echo -n "."
    sleep 0.2
done

echo ". Success!"

echo
echo "Ensuring leaf node is connected using container IP"
CONTAINER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${CONTAINER_NAME})
CURRENT_LEAF_IP=$(mysql -u root -h 127.0.0.1 -P 5506 --batch -N -e 'select host from information_schema.leaves')
if [[ ${CONTAINER_IP} != ${CURRENT_LEAF_IP} ]]; then
    # remove leaf with current ip
    mysql -u root -h 127.0.0.1 -P 5506 --batch -N -e "remove leaf '${CURRENT_LEAF_IP}':3307"
    # add leaf with correct ip
    mysql -u root -h 127.0.0.1 -P 5506 --batch -N -e "add leaf root@'${CONTAINER_IP}':3307"
fi
echo "Done!"
