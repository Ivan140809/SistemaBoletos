#!/usr/bin/env bash
# maquina 3 de almacenamiento, arranca persistencia y todo lo que se escriba despues del script se le pasa.
# sin argumentos carga data/test_case.json, con -generar inventa 50 eventos y con -inicial usa otro json.
# arquitectura escalable ./scripts/machine3_storage.sh -inicial data/test_case.json -workers 8
# ESCALABILIDAD VERTICAL, -workers dice cuantas peticiones atiende en paralelo esta instancia.
# ESCALABILIDAD HORIZONTAL, para mas instancias en otras maquinas usar scripts/persistence_extra.sh
# que no reinicia la base, esta de la m3 es la unica que debe reiniciarla.

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
# si no se paso ningun argumento usa el caso de prueba y reinicia la base
if [ $# -eq 0 ]; then
	# pone -inicial data/test_case.json como argumentos
	set -- -inicial data/test_case.json
fi
# arranca persistencia en el puerto 5560 con los argumentos que se pasaron
./bin/persistence -bind "tcp://*:5560" "$@"
