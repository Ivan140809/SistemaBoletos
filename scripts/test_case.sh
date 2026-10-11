#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${IP_NEGOCIO:?exporte IP_NEGOCIO con la IP de la máquina 2}"
GT="tcp://${IP_NEGOCIO}:5555"
MES="${MES:-2026-11}"

paso() {
	echo
	echo "=================================================================="
	echo " $1"
	echo "=================================================================="
	read -rp "Presione Enter para ejecutar..."
}

paso "1. Estado inicial de la BD"
./bin/client consultar -gt "$GT" -id Cliente1 -mes "$MES"

paso "2. Cliente1 reserva 5 asientos de E1"
./bin/client reservar -gt "$GT" -id Cliente1 -evento E1 -asientos 5

paso "3. Cliente2 reserva 3 asientos de E1"
./bin/client reservar -gt "$GT" -id Cliente2 -evento E1 -asientos 3

paso "4. Consulta de la BD: E1 debe tener 92 disponibles y 8 vendidos"
./bin/client consultar -gt "$GT" -id Cliente1 -mes "$MES"

paso "5. Clientes concurrentes reservan E2 (piden 130 > 100 asientos)"
./bin/client concurrente -gt "$GT" -evento E2 -asientos 30,25,40,20,15

paso "6. Consulta final: los puestos de E2 quedaron reservados"
./bin/client consultar -gt "$GT" -id Cliente1 -mes "$MES"
