#!/bin/bash
cd /storage/testroot
source venv/bin/activate
cd memsql-spark-connector

./scripts/test.py "$@"
RET=$?

if [[ -f /var/lib/memsql-ops/data/spark/install/super_app/super_app.log ]] && [[ ($RET -ne 0) ]]; then
    cat /var/lib/memsql-ops/data/spark/install/super_app/super_app.log
fi

exit $RET
