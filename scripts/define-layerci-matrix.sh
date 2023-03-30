#!/usr/bin/env bash
set -eu

TEST_NUM=${SPLIT:-"0"}

if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '1' ] || [ "$TEST_NUM" == '2' ] || [ "$TEST_NUM" == '3' ]
then
   echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:centos-7.5.12-3112a491c2-4.0.0-1.12.5"'
elif [ "$TEST_NUM" == '4' ] || [ "$TEST_NUM" == '5' ] || [ "$TEST_NUM" == '6' ] || [ "$TEST_NUM" == '7' ]
then
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-7.6.14-6f67cb4355-4.0.4-1.13.6"'
elif [ "$TEST_NUM" == '8'] || ["$TEST_NUM" == '9' ] || ["$TEST_NUM" == '10' ] || ["$TEST_NUM" == '11' ]
  then
    echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-7.8.19-4263b2d130-4.0.10-1.14.4"'
else
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-8.0.15-0b9b66384f-4.0.11-1.15.2"'
fi

if [ "$TEST_NUM" == '16' ]
then
  echo 'export FORCE_READ_FROM_LEAVES=TRUE'
else
  echo 'export FORCE_READ_FROM_LEAVES=FALSE'
fi

if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '4' ] || [ "$TEST_NUM" == '8' ] || [ "$TEST_NUM" == '12' ]
then
  echo 'export SPARK_VERSION="3.0.0"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark30"'
elif [ "$TEST_NUM" == '1' ] || [ "$TEST_NUM" == '5' ] || [ "$TEST_NUM" == '9' ] || [ "$TEST_NUM" == '13' ]
then
  echo 'export SPARK_VERSION="3.1.3"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark31"'
elif [ "$TEST_NUM" == '2' ] || [ "$TEST_NUM" == '6' ] || [ "$TEST_NUM" == '10' ] || [ "$TEST_NUM" == '14' ]
then
  echo 'export SPARK_VERSION="3.2.1"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark32"'
else
  echo 'export SPARK_VERSION="3.3.0"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark33"'
fi


echo 'export SCALA_VERSION="2.12.12"'
