#!/usr/bin/env bash
# demo basica que se corre en la m1, va paso a paso y espera enter para ir explicando.
# la m3 y la m2 deben estar arrancadas con data/test_case.json.
# uso IP_NEGOCIO=ip_m2 ./scripts/test_case.sh, ojo que aqui va la ip de la m2 y no la de la m3.

# se detiene ante cualquier error o variable sin definir
set -euo pipefail
# se para en la raiz del proyecto
cd "$(dirname "$0")/.."
# si falta IP_NEGOCIO se sale con este mensaje
: "${IP_NEGOCIO:?falta IP_NEGOCIO, la ip de la maquina 2}"
# direccion del gt en el puerto 5555 de la m2
GT="tcp://${IP_NEGOCIO}:5555"
# mes a consultar, por defecto 2026-11
MES="${MES:-2026-11}"

# imprime el texto del paso y espera enter
paso() {
	echo "$1"
	read -rp "enter para seguir"
}

# paso 1, estado inicial
paso "1. asi arranca la base"
./bin/client consultar -gt "$GT" -id Cliente1 -mes "$MES"
# paso 2, primera reserva
paso "2. Cliente1 reserva 5 asientos de E1"
./bin/client reservar -gt "$GT" -id Cliente1 -evento E1 -asientos 5
# paso 3, segunda reserva
paso "3. Cliente2 reserva 3 asientos de E1"
./bin/client reservar -gt "$GT" -id Cliente2 -evento E1 -asientos 3
# paso 4, revisa que E1 quedo con 92 libres
paso "4. volvemos a mirar la base, E1 deberia tener 92 libres y 8 vendidos"
./bin/client consultar -gt "$GT" -id Cliente1 -mes "$MES"
# paso 5, varios clientes compiten por E2
paso "5. varios clientes reservan E2 al mismo tiempo"
./bin/client concurrente -gt "$GT" -evento E2 -asientos 30,25,40,20,15
# paso 6, estado final
paso "6. asi queda la base al final"
./bin/client consultar -gt "$GT" -id Cliente1 -mes "$MES"
