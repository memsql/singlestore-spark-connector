#!/usr/bin/env bash
set -eu

TEST_NUM=${SPLIT:-"0"}

if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '1' ] || [ "$TEST_NUM" == '2' ] || [ "$TEST_NUM" == '3' ] || [ "$TEST_NUM" == '4' ] || [ "$TEST_NUM" == '5' ]
then
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-7.6.27-51e282b615-4.0.12-1.16.1"'
elif [ "$TEST_NUM" == '6' ] || [ "$TEST_NUM" == '7' ] || [ "$TEST_NUM" == '8' ] || [ "$TEST_NUM" == '9' ] || [ "$TEST_NUM" == '10' ] || [ "$TEST_NUM" == '11' ]
then
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-7.8.19-4263b2d130-4.0.10-1.14.4"'
elif [ "$TEST_NUM" == '12' ] || [ "$TEST_NUM" == '13' ] || [ "$TEST_NUM" == '14' ] || [ "$TEST_NUM" == '15' ] || [ "$TEST_NUM" == '16' ] || [ "$TEST_NUM" == '17' ]
then
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-8.0.19-f48780d261-4.0.11-1.16.0"'
else
  echo 'export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-8.1.26-810da32787-4.0.14-1.17.4"'
fi

if [ "$TEST_NUM" == '24' ]
then
  echo 'export FORCE_READ_FROM_LEAVES=TRUE'
else
  echo 'export FORCE_READ_FROM_LEAVES=FALSE'
fi

if [ "$TEST_NUM" == '0' ] || [ "$TEST_NUM" == '6' ] || [ "$TEST_NUM" == '12' ] || [ "$TEST_NUM" == '18' ]
then
  echo 'export SPARK_VERSION="3.0.3"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark30"'
elif [ "$TEST_NUM" == '1' ] || [ "$TEST_NUM" == '7' ] || [ "$TEST_NUM" == '13' ] || [ "$TEST_NUM" == '19' ]
then
  echo 'export SPARK_VERSION="3.1.3"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark31"'
elif [ "$TEST_NUM" == '2' ] || [ "$TEST_NUM" == '8' ] || [ "$TEST_NUM" == '14' ] || [ "$TEST_NUM" == '20' ]
then
  echo 'export SPARK_VERSION="3.2.4"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark32"'
elif [ "$TEST_NUM" == '3' ] || [ "$TEST_NUM" == '9' ] || [ "$TEST_NUM" == '15' ] || [ "$TEST_NUM" == '21' ]
then
  echo 'export SPARK_VERSION="3.3.3"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark33"'
elif [ "$TEST_NUM" == '4' ] || [ "$TEST_NUM" == '10' ] || [ "$TEST_NUM" == '16' ] || [ "$TEST_NUM" == '22' ]
then
  echo 'export SPARK_VERSION="3.4.1"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark34"'
else
  echo 'export SPARK_VERSION="3.5.0"'
  echo 'export TEST_FILTER="testOnly -- -l  ExcludeFromSpark35"'
fi


echo 'export SCALA_VERSION="2.12.12"'
