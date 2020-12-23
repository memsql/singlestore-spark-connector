#!/usr/bin/env bash
set -eu

TEST_NUM=${SPLIT:-"0"}

if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '1' ] || [ "$TEST_NUM" == '2' ]
then
  echo 'export MEMSQL_IMAGE="memsql/cluster-in-a-box:centos-7.0.15-619d118712-1.9.5-1.5.0"'
elif [ "$TEST_NUM" == '3' ] || [ "$TEST_NUM" == '4' ] || [ "$TEST_NUM" == '5' ]
then
  echo 'export MEMSQL_IMAGE="memsql/cluster-in-a-box:centos-6.8.15-029542cbf3-1.9.3-1.4.1"'
elif [ "$TEST_NUM" == '6' ] || [ "$TEST_NUM" == '7' ] || [ "$TEST_NUM" == '8' ]
then
  echo 'export MEMSQL_IMAGE="memsql/cluster-in-a-box:6.7.18-db1caffe94-1.6.1-1.1.1"'
elif [ "$TEST_NUM" == '9' ] || [ "$TEST_NUM" == '10' ] || [ "$TEST_NUM" == '11' ]
then
  echo 'export MEMSQL_IMAGE="memsql/cluster-in-a-box:centos-7.1.13-11ddea2a3a-3.0.0-1.9.3"'
else
  echo 'export MEMSQL_IMAGE="memsql/cluster-in-a-box:centos-7.3.2-a364d4b31f-3.0.0-1.9.3"'
fi


if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '3' ] || [ "$TEST_NUM" == '6' ] || [ "$TEST_NUM" == '9' ] || [ "$TEST_NUM" == '12' ]
then
  echo 'export SPARK_VERSION="3.0.0"'
  echo 'export SCALA_VERSION="2.12.12"'
  echo 'export TEST_FILTER="test"'
elif [ "$TEST_NUM" == '1' ] || [ "$TEST_NUM" == '4' ] || [ "$TEST_NUM" == '7' ] || [ "$TEST_NUM" == '10' ] || [ "$TEST_NUM" == '13' ]
then
  echo 'export SPARK_VERSION="2.4.4"'
  echo 'export SCALA_VERSION="2.11.11"'
  echo 'export TEST_FILTER="testOnly -- -l  OnlySpark3"'
else
  echo 'export SPARK_VERSION="2.3.4"'
  echo 'export SCALA_VERSION="2.11.11"'
  echo 'export TEST_FILTER="testOnly -- -l  OnlySpark3"'
fi

if [ "$TEST_NUM" -gt 8 ]
then
  echo 'export MEMSQL_PASSWORD="password"'
fi

