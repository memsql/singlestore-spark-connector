#!/usr/bin/env bash
set -eu

TEST_NUM=${SPLIT:-"0"}

if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '1' ] || [ "$TEST_NUM" == '2']
then
   echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.3.2-a364d4b31f-3.0.0-1.9.3"'
elif [ "$TEST_NUM" == '3' ] || [ "$TEST_NUM" == '4'] || [ "$TEST_NUM" == '5']
then
  echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.5.8-12c73130aa-3.2.11-1.11.11"'
elif [ "$TEST_NUM" == '6'] || ["$TEST_NUM" == '7' ] || [ "$TEST_NUM" == '8' ]
  then
    echo 'export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.6.5-018454f4e3-4.0.1-1.13.0"'
else
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-7.8.2-8c7b918527-4.0.4-1.13.6"'
fi

if [ "$TEST_NUM" == '12' ]
then
  echo 'export FORCE_READ_FROM_LEAVES=TRUE'
else
  echo 'export FORCE_READ_FROM_LEAVES=FALSE'
fi

if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '3' ] || [ "$TEST_NUM" == '6' ] || [ "$TEST_NUM" == '9' ]
then
  echo 'export SPARK_VERSION="3.0.0"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark30"'
elif [ "$TEST_NUM" == '1' ] || [ "$TEST_NUM" == '4' ] || [ "$TEST_NUM" == '7' ] || [ "$TEST_NUM" == '10' ]
then
  echo 'export SPARK_VERSION="3.1.0"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark31"'
else
  echo 'export SPARK_VERSION="3.2.0"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark32"'
fi

echo 'export SCALA_VERSION="2.12.12"'
