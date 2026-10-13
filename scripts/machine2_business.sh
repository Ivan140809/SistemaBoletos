#!/usr/bin/env bash
# maquina 2 (negocio): arranca el gt, rmc y cancelaciones juntos en la misma terminal.
# uso:   IP_ALMACEN=<ip_de_la_m3> ./scripts/machine2_business.sh
# (IP_ALMACEN es la ip de la maquina 3, donde corre persistencia)
set -euo pipefail
cd "$(dirname "$0")/.."
# si IP_ALMACEN no esta definida, se sale con ese mensaje de error (sintaxis ${VAR:?mensaje}).
: "${IP_ALMACEN:?falta IP_ALMACEN, la ip de la maquina 3}"
# al salir del script (ctrl+c incluido) mata los procesos que dejo en segundo plano.
trap 'kill $(jobs -p) 2>/dev/null' EXIT
# el "&" al final de la linea lanza el programa en segundo plano.
./bin/manager &
# pausa corta para que el gt alcance a abrir sus puertos antes de que los demas se conecten.
sleep 0.5
./bin/rmc -gt tcp://localhost:5556 -persistencia "tcp://${IP_ALMACEN}:5560" &
./bin/cancellations -gt tcp://localhost:5557 &
# espera a que terminen los procesos en segundo plano (o sea, se queda corriendo).
wait
