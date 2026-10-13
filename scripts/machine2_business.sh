#!/usr/bin/env bash
# MÁQUINA 2 (negocio): arranca el Gestor de Transacciones, RMC y Cancelaciones
# juntos en la misma terminal.
# Uso:   IP_ALMACEN=<IP_de_M3> ./scripts/machine2_business.sh
# (IP_ALMACEN = dirección IP de la máquina 3, donde corre persistencia)
set -euo pipefail
cd "$(dirname "$0")/.."
# Si IP_ALMACEN no está definida, termina con ese mensaje de error (la sintaxis ${VAR:?mensaje}).
: "${IP_ALMACEN:?falta IP_ALMACEN, la ip de la maquina 3}"
# Al salir el script (Ctrl+C incluido), mata los procesos que dejó en segundo plano.
trap 'kill $(jobs -p) 2>/dev/null' EXIT
# El "&" al final de una línea lanza el programa en segundo plano.
./bin/manager &
# Pausa corta para que el GT alcance a abrir sus puertos antes de que los demás se conecten.
sleep 0.5
./bin/rmc -gt tcp://localhost:5556 -persistencia "tcp://${IP_ALMACEN}:5560" &
./bin/cancellations -gt tcp://localhost:5557 &
# Espera a que terminen los procesos en segundo plano (o sea, se queda corriendo).
wait
