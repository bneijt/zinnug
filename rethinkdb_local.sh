#!/bin/bash
ROOT="`pwd`"
BASEPATH="${ROOT}/rethinkdb"
if [ ! -d "${BASEPATH}" ]; then
    mkdir -p "${BASEPATH}"
    rethinkdb create --no-update-check --log-file "rethinkdb.log" --directory "${BASEPATH}"
fi
exec rethinkdb serve --no-update-check --log-file "rethinkdb.log" --directory "${BASEPATH}"
