#!/usr/bin/env bash

# Author: Liran Funaro <liran.funaro@ibm.com>

orion_log_dir="/data/orion-benchmark-data"
orion_log="${orion_log_dir}/orion.log"

make
bin/orion-bench -config examples/config.yaml -clear -material
mkdir -p "${orion_log_dir}"
bin/bdb start \
    --configpath "/data/orion-benchmark-material/server-main.local-config.yml" \
    >"${orion_log}" 2>&1 &
ORION_PID=$!

trap ctrl_c INT
function ctrl_c() {
  echo "### User enter CTRL-C"
  kill $ORION_PID
}

until [ -f "${orion_log}" ]; do
  sleep 0.1
done

tail -f "${orion_log}" | sed '/Leader changed/ q'

bin/orion-bench -config examples/config.yaml -init -run

kill $ORION_PID
