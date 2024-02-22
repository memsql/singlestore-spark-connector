#!/usr/bin/env bash
set -eu

# this script must be run from the top-level of the repo
cd "$(git rev-parse --show-toplevel)"

DEFAULT_IMAGE_NAME="singlestore/cluster-in-a-box:alma-8.0.15-0b9b66384f-4.0.11-1.15.2"
IMAGE_NAME="${SINGLESTORE_IMAGE:-$DEFAULT_IMAGE_NAME}"
CONTAINER_NAME="singlestore-integration"

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
        -v ${PWD}/scripts/ssl:/test-ssl \
        -v ${PWD}/scripts/jwt:/test-jwt \
        -e LICENSE_KEY=${LICENSE_KEY} \
        -e ROOT_PASSWORD=${SINGLESTORE_PASSWORD} \
        -p 5506:3306 -p 5507:3307 -p 5508:3308 \
        ${IMAGE_NAME}
fi

docker start ${CONTAINER_NAME}

singlestore-wait-start() {
  echo -n "Waiting for SingleStore to start..."
  while true; do
      if mysql -u root -h 127.0.0.1 -P 5506 -p"${SINGLESTORE_PASSWORD}" -e "select 1" >/dev/null 2>/dev/null; then
          break
      fi
      echo -n "."
      sleep 0.2
  done
  echo ". Success!"
}

singlestore-wait-start

if [[ "${EXISTS}" -eq 0 ]]; then
    echo
    echo "Creating aggregator node"
    docker exec -it ${CONTAINER_NAME} memsqlctl create-node --yes --password ${SINGLESTORE_PASSWORD} --port 3308
    docker exec -it ${CONTAINER_NAME} memsqlctl update-config --yes --all --key minimum_core_count --value 0
    docker exec -it ${CONTAINER_NAME} memsqlctl update-config --yes --all --key minimum_memory_mb --value 0
    docker exec -it ${CONTAINER_NAME} memsqlctl start-node --yes --all
    docker exec -it ${CONTAINER_NAME} memsqlctl add-aggregator --yes --host 127.0.0.1 --password ${SINGLESTORE_PASSWORD} --port 3308
fi

echo
echo "Setting up SSL"
docker exec -it ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_ca --value /test-ssl/test-ca-cert.pem
docker exec -it ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_cert --value /test-ssl/test-singlestore-cert.pem
docker exec -it ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_key --value /test-ssl/test-singlestore-key.pem
echo "Setting up JWT"
docker exec -it ${CONTAINER_NAME} memsqlctl update-config --yes --all --key jwt_auth_config_file --value /test-jwt/jwt_auth_config.json
echo "Restarting cluster"
docker exec -it ${CONTAINER_NAME} memsqlctl restart-node --yes --all
singlestore-wait-start
echo "Setting up root-ssl user"
mysql -u root -h 127.0.0.1 -P 5506 -p"${SINGLESTORE_PASSWORD}" -e 'create user "root-ssl"@"%" require ssl'
mysql -u root -h 127.0.0.1 -P 5506 -p"${SINGLESTORE_PASSWORD}" -e 'grant all privileges on *.* to "root-ssl" with grant option'
mysql -u root -h 127.0.0.1 -P 5507 -p"${SINGLESTORE_PASSWORD}" -e 'create user "root-ssl"@"%" require ssl'
mysql -u root -h 127.0.0.1 -P 5507 -p"${SINGLESTORE_PASSWORD}" -e 'grant all privileges on *.* to "root-ssl" with grant option'
mysql -u root -h 127.0.0.1 -P 5508 -p"${SINGLESTORE_PASSWORD}" -e 'grant all privileges on *.* to "root-ssl" with grant option'
echo "Done!"
echo "Setting up root-jwt user"
mysql -h 127.0.0.1 -u root -P 5506 -p"${SINGLESTORE_PASSWORD}" -e "CREATE USER 'test_jwt_user' IDENTIFIED WITH authentication_jwt"
mysql -h 127.0.0.1 -u root -P 5506 -p"${SINGLESTORE_PASSWORD}" -e "GRANT ALL PRIVILEGES ON *.* TO 'test_jwt_user'@'%'"
echo "Done!"

echo
echo "Ensuring child nodes are connected using container IP"
CONTAINER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${CONTAINER_NAME})
CURRENT_LEAF_IP=$(mysql -u root -h 127.0.0.1 -P 5506 -p"${SINGLESTORE_PASSWORD}" --batch -N -e 'select host from information_schema.leaves')
if [[ ${CONTAINER_IP} != "${CURRENT_LEAF_IP}" ]]; then
    # remove leaf with current ip
    mysql -u root -h 127.0.0.1 -P 5506 -p"${SINGLESTORE_PASSWORD}" --batch -N -e "remove leaf '${CURRENT_LEAF_IP}':3307"
    # add leaf with correct ip
    mysql -u root -h 127.0.0.1 -P 5506 -p"${SINGLESTORE_PASSWORD}" --batch -N -e "add leaf root:'${SINGLESTORE_PASSWORD}'@'${CONTAINER_IP}':3307"
fi
CURRENT_AGG_IP=$(mysql -u root -h 127.0.0.1 -P 5506 -p"${SINGLESTORE_PASSWORD}" --batch -N -e 'select host from information_schema.aggregators where master_aggregator=0')
if [[ ${CONTAINER_IP} != "${CURRENT_AGG_IP}" ]]; then
    # remove aggregator with current ip
    mysql -u root -h 127.0.0.1 -P 5506 -p"${SINGLESTORE_PASSWORD}" --batch -N -e "remove aggregator '${CURRENT_AGG_IP}':3308"
    # add aggregator with correct ip
    mysql -u root -h 127.0.0.1 -P 5506 -p"${SINGLESTORE_PASSWORD}" --batch -N -e "add aggregator root:'${SINGLESTORE_PASSWORD}'@'${CONTAINER_IP}':3308"
fi
echo "Done!"
