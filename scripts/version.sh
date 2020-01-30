#!/bin/sh
set -e

if [ -n "${CI_COMMIT_SHA}" ]; then
    echo "${CI_COMMIT_SHA}"
else
    git describe --dirty=-dirty --always --long --abbrev=40 --match=''
fi
