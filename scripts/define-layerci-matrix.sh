#!/usr/bin/env bash
set -eu

TEST_NUM=${SPLIT:-"0"}

if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '1' ]
then
  echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.0.15-619d118712-1.9.5-1.5.0"'
elif [ "$TEST_NUM" == '2' ] || [ "$TEST_NUM" == '3' ]
then
  echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.1.13-11ddea2a3a-3.0.0-1.9.3"'
  echo 'export SINGLESTORE_PASSWORD="password"'
elif [ "$TEST_NUM" == '4' ] || [ "$TEST_NUM" == '5' ]
then
  echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.3.2-a364d4b31f-3.0.0-1.9.3"'
  echo 'export SINGLESTORE_PASSWORD="password"'
else
  echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.5.8-12c73130aa-3.2.11-1.11.11"'
  echo 'export SINGLESTORE_PASSWORD="password"'
fi

if [ "$TEST_NUM" == '9' ]
then
  echo 'export FORCE_READ_FROM_LEAVES=TRUE'
else
  echo 'export FORCE_READ_FROM_LEAVES=FALSE'
fi

if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '2' ] || [ "$TEST_NUM" == '4' ] || [ "$TEST_NUM" == '6' ]
then
  echo 'export SPARK_VERSION="3.0.0"'
  echo 'export TEST_FILTER="testOnly -- -l  OnlySpark31"'
else
  echo 'export SPARK_VERSION="3.1.0"'
  echo 'export TEST_FILTER="testOnly -- -l  OnlySpark30"'
fi
echo 'export SCALA_VERSION="2.12.12"'
