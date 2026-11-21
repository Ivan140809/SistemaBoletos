#!/usr/bin/env bash
# maquina 2 de negocio, arranca el gt, las instancias de rmc y cancelaciones en la misma terminal.
# IP_ALMACEN es la ip de la maquina donde corre persistencia, pueden ser varias separadas por coma.
# arquitectura escalable con 3 rmc y 2 persistencias IP_ALMACEN=ip_m3,ip_m4 RMCS=3 ./scripts/machine2_business.sh

# se detiene ante cualquier error o variable sin definir
set -euo pipefail
# se para en la raiz del proyecto
cd "$(dirname "$0")/.."
# si falta IP_ALMACEN se sale con este mensaje
: "${IP_ALMACEN:?falta IP_ALMACEN, la ip de la maquina 3 (o varias separadas por coma)}"
# ESCALABILIDAD HORIZONTAL, cuantas instancias de rmc lanzar, por defecto 3
RMCS="${RMCS:-3}"
# ESCALABILIDAD HORIZONTAL, parte IP_ALMACEN por comas y guarda cada ip en una lista
IFS=, read -ra IPS <<< "$IP_ALMACEN"
# aqui se crea la lista de direcciones de persistencia, empieza vacia
PERSISTENCIA=""
# recorre cada ip
for ip in "${IPS[@]}"; do
	# le agrega tcp y el puerto 5560 y la suma a la lista separada por coma
	PERSISTENCIA="${PERSISTENCIA:+$PERSISTENCIA,}tcp://${ip}:5560"
done
# al salir, incluso con ctrl+c, mata los procesos que quedaron en segundo plano
trap 'kill $(jobs -p) 2>/dev/null' EXIT
# arranca el gt en segundo plano
./bin/manager &
# espera un momento para que el gt abra sus puertos
sleep 0.5
# ESCALABILIDAD HORIZONTAL, lanza varios rmc conectados al mismo gt que les reparte por turnos
for i in $(seq 1 "$RMCS"); do
	# cada rmc tiene su nombre y reparte sus peticiones entre todas las persistencias
	./bin/rmc -id "RMC-$i" -gt tcp://localhost:5556 -persistencia "$PERSISTENCIA" &
done
# arranca cancelaciones en segundo plano
./bin/cancellations -gt tcp://localhost:5557 &
# se queda corriendo hasta que terminen los procesos
wait
