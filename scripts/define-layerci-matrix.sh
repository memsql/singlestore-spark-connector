#!/usr/bin/env bash
set -eu

TEST_NUM=${SPLIT:-"0"}

if [ "$TEST_NUM" == '0' ]
then
  echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.0.15-619d118712-1.9.5-1.5.0"'
elif [ "$TEST_NUM" == '1' ]
then
  echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-6.8.15-029542cbf3-1.9.3-1.4.1"'
elif [ "$TEST_NUM" == '2' ]
then
  echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:6.7.18-db1caffe94-1.6.1-1.1.1"'
elif [ "$TEST_NUM" == '3' ]
then
  echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.1.13-11ddea2a3a-3.0.0-1.9.3"'
else
  echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.3.2-a364d4b31f-3.0.0-1.9.3"'
fi


echo 'export SPARK_VERSION="2.4.7"'
echo 'export SCALA_VERSION="2.11.11"'
echo 'export TEST_FILTER="testOnly -- -l  OnlySpark3"'

if [ "$TEST_NUM" -gt 2 ]
then
  echo 'export SINGLESTORE_PASSWORD="password"'
fi

