#!/usr/bin/env bash
# maquina 2, arranca el gt, los rmc y cancelaciones. uso IP_ALMACEN=ip_m3,ip_m4 RMCS=3 ./scripts/machine2_business.sh
set -euo pipefail
cd "$(dirname "$0")/.."
: "${IP_ALMACEN:?falta IP_ALMACEN, la ip de la maquina 3 (o varias separadas por coma)}"
# REPLICAS de rmc, cuantas copias se lanzan
RMCS="${RMCS:-3}"
# REPLICAS de persistencia, una direccion por cada ip de IP_ALMACEN
IFS=, read -ra IPS <<< "$IP_ALMACEN"
PERSISTENCIA=""
for ip in "${IPS[@]}"; do
	PERSISTENCIA="${PERSISTENCIA:+$PERSISTENCIA,}tcp://${ip}:5560"
done
trap 'kill $(jobs -p) 2>/dev/null' EXIT
./bin/manager &
sleep 0.5
# REPLICAS de rmc, aqui se lanzan todas conectadas al mismo gt y a todas las persistencias
for i in $(seq 1 "$RMCS"); do
	./bin/rmc -id "RMC-$i" -gt tcp://localhost:5556 -persistencia "$PERSISTENCIA" &
done
# arranca cancelaciones en segundo plano, tambien habla con todas las persistencias
./bin/cancellations -gt tcp://localhost:5557 -persistencia "$PERSISTENCIA" &
wait
