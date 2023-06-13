#!/usr/bin/env bash
set -eu

TEST_NUM=${SPLIT:-"0"}

if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '1' ] || [ "$TEST_NUM" == '2' ] || [ "$TEST_NUM" == '3' ] || [ "$TEST_NUM" == '4' ]
then
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:centos-7.5.12-3112a491c2-4.0.0-1.12.5"'
elif [ "$TEST_NUM" == '5' ] || [ "$TEST_NUM" == '6' ] || [ "$TEST_NUM" == '7' ] || [ "$TEST_NUM" == '8' ] || [ "$TEST_NUM" == '9' ]
then
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-7.6.14-6f67cb4355-4.0.4-1.13.6"'
elif [ "$TEST_NUM" == '10' ] || [ "$TEST_NUM" == '11' ] || [ "$TEST_NUM" == '12' ] || [ "$TEST_NUM" == '13' ] || [ "$TEST_NUM" == '14' ]
then
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-7.8.19-4263b2d130-4.0.10-1.14.4"'
elif [ "$TEST_NUM" == '15' ] || [ "$TEST_NUM" == '16' ] || [ "$TEST_NUM" == '17' ] || [ "$TEST_NUM" == '18' ] || [ "$TEST_NUM" == '19' ]
then
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-8.0.19-f48780d261-4.0.11-1.16.0"'
else
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-8.1.6-74913b66cc-4.0.11-1.16.0"'
fi

if [ "$TEST_NUM" == '25' ]
then
  echo 'export FORCE_READ_FROM_LEAVES=TRUE'
else
  echo 'export FORCE_READ_FROM_LEAVES=FALSE'
fi

if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '5' ] || [ "$TEST_NUM" == '10' ] || [ "$TEST_NUM" == '15' ] || [ "$TEST_NUM" == '20' ]
then
  echo 'export SPARK_VERSION="3.0.3"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark30"'
elif [ "$TEST_NUM" == '1' ] || [ "$TEST_NUM" == '6' ] || [ "$TEST_NUM" == '11' ] || [ "$TEST_NUM" == '16' ] || [ "$TEST_NUM" == '21' ]
then
  echo 'export SPARK_VERSION="3.1.3"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark31"'
elif [ "$TEST_NUM" == '2' ] || [ "$TEST_NUM" == '7' ] || [ "$TEST_NUM" == '12' ] || [ "$TEST_NUM" == '17' ] || [ "$TEST_NUM" == '22' ]
then
  echo 'export SPARK_VERSION="3.2.4"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark32"'
elif [ "$TEST_NUM" == '3' ] || [ "$TEST_NUM" == '8' ] || [ "$TEST_NUM" == '13' ] || [ "$TEST_NUM" == '18' ] || [ "$TEST_NUM" == '23' ]
then
  echo 'export SPARK_VERSION="3.3.2"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark33"'
else
  echo 'export SPARK_VERSION="3.4.0"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark34"'
fi


echo 'export SCALA_VERSION="2.12.12"'
