#!/usr/bin/env bash
# demo basica (se corre en la m1, el cliente).
# va paso a paso: cada "paso" muestra una descripcion y espera enter, para poder
# ir explicando lo que pasa en la sustentacion.
# requisito: la m3 y la m2 ya arrancadas (con data/test_case.json).
# uso:   IP_NEGOCIO=<ip_de_la_m2> ./scripts/test_case.sh
# (ojo: este script usa IP_NEGOCIO, la ip de la m2; machine2_business.sh usa IP_ALMACEN, la ip de la m3.)
set -euo pipefail
cd "$(dirname "$0")/.."
: "${IP_NEGOCIO:?falta IP_NEGOCIO, la ip de la maquina 2}"
# direccion del gt (puerto 5555 de la m2).
GT="tcp://${IP_NEGOCIO}:5555"
# mes a consultar, se cambia con la variable MES (por defecto 2026-11).
MES="${MES:-2026-11}"

# ayudante: imprime el texto del paso ($1) y espera a que le des enter.
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
