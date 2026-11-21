#!/usr/bin/env bash
# instancia extra de persistencia para la m4 u otra maquina, usa la misma base que la m3.
# nunca reinicia la base, por eso no acepta -inicial ni -generar, eso solo lo hace la m3.
# uso ./scripts/persistence_extra.sh -workers 8, y en la m2 IP_ALMACEN=ip_m3,ip_m4
# ESCALABILIDAD HORIZONTAL, cada instancia mas es otra direccion a la que el req de rmc reparte.

# se detiene ante cualquier error, variable sin definir o fallo en una tuberia
set -euo pipefail
# se para en la raiz del proyecto
cd "$(dirname "$0")/.."
# si existe el .env carga sus variables porque go no lo lee solo
if [ -f .env ]; then
	# todo lo que se defina queda exportado
	set -a
	# carga DATABASE_URL y las demas variables
	source .env
	# deja de exportar automaticamente
	set +a
fi
# revisa cada argumento
for arg in "$@"; do
	# si alguien intenta reiniciar la base se detiene
	case "$arg" in
	-inicial | --inicial | -inicial=* | --inicial=* | -generar | --generar | -generar=* | --generar=*)
		echo "persistence_extra.sh no reinicia la base, eso solo lo hace machine3_storage.sh" >&2
		exit 1
		;;
	esac
done
# puerto donde escucha esta instancia, por defecto 5560
PUERTO="${PUERTO:-5560}"
# arranca persistencia sin tocar los datos que ya hay
./bin/persistence -bind "tcp://*:${PUERTO}" "$@"
