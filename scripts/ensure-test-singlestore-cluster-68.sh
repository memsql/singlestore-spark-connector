#!/usr/bin/env bash
set -eu
export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-6.8.15-029542cbf3-1.9.3-1.4.1"
./scripts/ensure-test-singlestore-cluster.sh
