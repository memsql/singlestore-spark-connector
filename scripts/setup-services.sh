#!/usr/bin/env sh
set -e

export ROOT_DIR=$(dirname $(dirname $(readlink -f "$0")))
export MYSQL_BIN=$(which mysql)

MASTER_IP=$(getent hosts memsql-master | awk '{ print $1 }')
LEAF_IP=$(getent hosts memsql-leaf | awk '{ print $1 }')

memsql() {
    local role=${1}
    shift
    mysql -u root -h memsql-${role} "${@}"
}

memquery() {
    local role=${1}
    shift
    memsql ${role} -e "${@}"
}

echo "status check"
memquery master "SELECT 'memsql master OK' AS status"
memquery leaf "SELECT 'memsql leaf OK' AS status"

echo "setup memsql cluster"
memquery master "SET LICENSE = '${LICENSE_KEY}'"
memquery master "BOOTSTRAP AGGREGATOR '${MASTER_IP}'"
memquery master "ADD LEAF 'root'@'${LEAF_IP}'"

echo "inspect memsql cluster"
memquery master "SHOW AGGREGATORS"
memquery master "SHOW LEAVES"

echo "initialize memsql database"
memquery master "CREATE DATABASE tdb"
