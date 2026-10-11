#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${IP_ALMACEN:?exporte IP_ALMACEN con la IP de la máquina 3}"
trap 'kill $(jobs -p) 2>/dev/null' EXIT
./bin/manager &
sleep 0.5
./bin/rmc -gt tcp://localhost:5556 -persistencia "tcp://${IP_ALMACEN}:5560" &
./bin/cancellations -gt tcp://localhost:5557 -persistencia "tcp://${IP_ALMACEN}:5560" &
wait
