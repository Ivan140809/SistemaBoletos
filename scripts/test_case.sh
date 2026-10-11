#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${IP_NEGOCIO:?falta IP_NEGOCIO, la ip de la maquina 2}"
GT="tcp://${IP_NEGOCIO}:5555"
MES="${MES:-2026-11}"

paso() {
	echo "$1"
	read -rp "enter para seguir"
}

paso "1. asi arranca la base"
./bin/client consultar -gt "$GT" -id Cliente1 -mes "$MES"
paso "2. Cliente1 reserva 5 asientos de E1"
./bin/client reservar -gt "$GT" -id Cliente1 -evento E1 -asientos 5
paso "3. Cliente2 reserva 3 asientos de E1"
./bin/client reservar -gt "$GT" -id Cliente2 -evento E1 -asientos 3
paso "4. volvemos a mirar la base, E1 deberia tener 92 libres y 8 vendidos"
./bin/client consultar -gt "$GT" -id Cliente1 -mes "$MES"
paso "5. varios clientes reservan E2 al mismo tiempo"
./bin/client concurrente -gt "$GT" -evento E2 -asientos 30,25,40,20,15
paso "6. asi queda la base al final"
./bin/client consultar -gt "$GT" -id Cliente1 -mes "$MES"
