#!/usr/bin/env bash
# TOLERANCIA A FALLAS DEL GT, gt de respaldo en otra maquina (binary star).
# vigila al gt primario de la maquina 2 y toma el control si el primario deja de dar senales.
# uso IP_PRIMARIO=ip_m2 ./scripts/gt_respaldo.sh
# en la m2 se arranca el primario con GT_RESPALDO=<ip de esta maquina> ./scripts/machine2_business.sh
# esta maquina debe dejar entrar los puertos 5555 a 5558 (sudo ufw allow 5555:5558/tcp)
set -euo pipefail
cd "$(dirname "$0")/.."
: "${IP_PRIMARIO:?falta IP_PRIMARIO, la ip de la maquina 2 donde corre el gt primario}"
./bin/manager -rol respaldo -par "tcp://${IP_PRIMARIO}:5558" "$@"
