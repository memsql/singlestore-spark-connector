#!/bin/bash
cd /storage/testroot
source venv/bin/activate
cd memsql-spark-connector

./scripts/test.py "$@"
RET=$?

if [[ -f /var/lib/memsql-ops/data/spark/install/interface/interface.log ]] && [[ ($RET -ne 0) ]]; then
    cat /var/lib/memsql-ops/data/spark/install/interface/interface.log
fi

exit $RET
