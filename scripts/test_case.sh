#!/usr/bin/env bash
# ESCENARIO DE DEMOSTRACIÓN BÁSICO (se ejecuta en la M1, el cliente).
# Va paso a paso: cada "paso" muestra una descripción y espera ENTER, para que
# se pueda explicar lo que ocurre en la sustentación.
# Requisito: M3 y M2 ya arrancadas (con data/test_case.json).
# Uso:   IP_NEGOCIO=<IP_de_M2> ./scripts/test_case.sh
# (Ojo: este script usa IP_NEGOCIO, la IP de M2; machine2_business.sh usa IP_ALMACEN, la IP de M3.)
set -euo pipefail
cd "$(dirname "$0")/.."
: "${IP_NEGOCIO:?falta IP_NEGOCIO, la ip de la maquina 2}"
# Dirección del Gestor de Transacciones (puerto 5555 de la M2).
GT="tcp://${IP_NEGOCIO}:5555"
# Mes a consultar: se puede cambiar con la variable MES (por defecto 2026-11).
MES="${MES:-2026-11}"

# Función auxiliar: imprime el texto del paso ($1) y pausa hasta que se presione ENTER.
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
