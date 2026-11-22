#!/usr/bin/env bash
# maquina 2, arranca el gt, los rmc y cancelaciones. uso IP_ALMACEN=ip_m3,ip_m4 RMCS=3 ./scripts/machine2_business.sh
# TOLERANCIA A FALLAS DEL GT, con GT_RESPALDO se arranca tambien un gt de respaldo (binary star):
#   GT_RESPALDO=local     el respaldo corre en esta misma maquina en los puertos 5565 a 5568
#   GT_RESPALDO=<ip>      el respaldo corre en otra maquina con scripts/gt_respaldo.sh en los puertos 5555 a 5558
# sin GT_RESPALDO corre un solo gt como antes
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

# TOLERANCIA A FALLAS DEL GT, direcciones de los gt a las que se conectan rmc y cancelaciones
GT_RMC="tcp://localhost:5556"
GT_CANCELACIONES="tcp://localhost:5557"
RESPALDO="${GT_RESPALDO:-}"
if [ -z "$RESPALDO" ]; then
	# un solo gt
	./bin/manager &
elif [ "$RESPALDO" = "local" ]; then
	# primario y respaldo en esta maquina, cada uno vigila el estado del otro
	./bin/manager -rol primario -par tcp://localhost:5568 &
	./bin/manager -rol respaldo -clientes "tcp://*:5565" -rmc "tcp://*:5566" -cancelaciones "tcp://*:5567" -estado "tcp://*:5568" -par tcp://localhost:5558 &
	GT_RMC="$GT_RMC,tcp://localhost:5566"
	GT_CANCELACIONES="$GT_CANCELACIONES,tcp://localhost:5567"
else
	# el respaldo corre en otra maquina, aqui solo el primario
	./bin/manager -rol primario -par "tcp://${RESPALDO}:5558" &
	GT_RMC="$GT_RMC,tcp://${RESPALDO}:5556"
	GT_CANCELACIONES="$GT_CANCELACIONES,tcp://${RESPALDO}:5557"
fi
sleep 0.5
# REPLICAS de rmc, aqui se lanzan todas conectadas a los gt y a todas las persistencias
for i in $(seq 1 "$RMCS"); do
	./bin/rmc -id "RMC-$i" -gt "$GT_RMC" -persistencia "$PERSISTENCIA" &
done
# arranca cancelaciones en segundo plano, recibe de los gt y habla con todas las persistencias
./bin/cancellations -gt "$GT_CANCELACIONES" -persistencia "$PERSISTENCIA" &
wait
