#!/usr/bin/env bash
# REPLICA de persistencia para la m4 u otra maquina, misma base que la m3 y nunca la reinicia.
# uso ./scripts/persistence_extra.sh -workers 8, y en la m2 se agrega su ip en IP_ALMACEN=ip_m3,ip_m4
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then
	set -a
	source .env
	set +a
fi
for arg in "$@"; do
	case "$arg" in
	-inicial | --inicial | -inicial=* | --inicial=* | -generar | --generar | -generar=* | --generar=*)
		echo "persistence_extra.sh no reinicia la base, eso solo lo hace machine3_storage.sh" >&2
		exit 1
		;;
	esac
done
PUERTO="${PUERTO:-5560}"
# aqui arranca la REPLICA de persistencia
./bin/persistence -bind "tcp://*:${PUERTO}" "$@"
