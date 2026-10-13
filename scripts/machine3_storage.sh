#!/usr/bin/env bash
# MÁQUINA 3 (almacenamiento): arranca el servicio de persistencia.
# Uso:   ./scripts/machine3_storage.sh                         (carga data/test_case.json)
#        ./scripts/machine3_storage.sh -generar                (50 eventos, 20 con dos fechas, 100 asientos)
#        ./scripts/machine3_storage.sh -inicial data/otro.json (un caso de prueba propio)
# Todo lo que se escriba después del nombre del script se le pasa a ./bin/persistence.

# set -e: terminar si algún comando falla. set -u: error si se usa una variable sin definir.
# set -o pipefail: un fallo dentro de una tubería (|) también cuenta como fallo.
set -euo pipefail
# Moverse a la carpeta raíz del proyecto, sin importar desde dónde se ejecute el script.
cd "$(dirname "$0")/.."
# Si existe el archivo .env, cargar sus variables (DATABASE_URL) al entorno.
# "set -a" hace que todo lo que se defina quede exportado; "source" ejecuta el archivo.
# Go no lee el .env por sí mismo, por eso este paso es necesario.
if [ -f .env ]; then
	set -a
	source .env  
	set +a
fi
# $# = cantidad de argumentos recibidos. Si no se pasó ninguno, se usa por defecto
# "-inicial data/test_case.json". Ese es el motivo por el que, al ejecutar el
# script sin argumentos, la base se borra y se vuelve a cargar desde ese archivo.
# (Si se pasa CUALQUIER argumento, ese valor por defecto deja de aplicarse.)
if [ $# -eq 0 ]; then
	set -- -inicial data/test_case.json
fi
# "$@" = todos los argumentos tal cual. Se arranca persistencia escuchando en el puerto 5560.
./bin/persistence -bind "tcp://*:5560" "$@"
