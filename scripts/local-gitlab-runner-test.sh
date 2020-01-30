#!/usr/bin/env bash
set -u

# this script must be run from the top-level of the repo
cd $(git rev-parse --show-toplevel)

VERSION=$(./scripts/version.sh)

# build a new test-runner
docker build -t psy3.memcompute.com/memsql-spark-utils/test-runner:${VERSION} .

# run tests locally using gitlab runner
docker run --rm -it \
    -w ${PWD} \
    -v ${PWD}:${PWD} \
    -v /var/run/docker.sock:/var/run/docker.sock \
    gitlab/gitlab-runner exec docker test
