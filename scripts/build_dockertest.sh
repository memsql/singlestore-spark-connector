#!/bin/bash

IMAGE=memsql_spark_connector/dockertest:latest

docker build -t $IMAGE . 1>&2
[ $? -eq 0 ] || exit 1

echo $IMAGE
