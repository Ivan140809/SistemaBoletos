#!/usr/bin/env bash
# maquina 3 (almacenamiento): arranca el servicio de persistencia.
# uso:   ./scripts/machine3_storage.sh                         (carga data/test_case.json)
#        ./scripts/machine3_storage.sh -generar                (50 eventos, 20 con dos fechas, 100 asientos)
#        ./scripts/machine3_storage.sh -inicial data/otro.json (un caso de prueba propio)
# todo lo que escribas despues del nombre del script se le pasa a ./bin/persistence.

# set -e: parar si algun comando falla. set -u: error si se usa una variable sin definir.
# set -o pipefail: un fallo dentro de una tuberia (|) tambien cuenta como fallo.
set -euo pipefail
# ir a la carpeta raiz del proyecto, sin importar desde donde se corra el script.
cd "$(dirname "$0")/.."
# si existe el .env, carga sus variables (DATABASE_URL) al entorno.
# "set -a" hace que todo lo que se defina quede exportado y "source" ejecuta el archivo.
# go no lee el .env solo, por eso hace falta este paso.
if [ -f .env ]; then
	set -a
	source .env  
	set +a
fi
# $# es la cantidad de argumentos recibidos. si no se paso ninguno, se usa por defecto
# "-inicial data/test_case.json". por eso, al correr el script sin argumentos, la
# base se borra y se vuelve a cargar desde ese archivo.
# (si se pasa CUALQUIER argumento, ese valor por defecto ya no aplica.)
if [ $# -eq 0 ]; then
	set -- -inicial data/test_case.json
fi
# "$@" son todos los argumentos tal cual. arranca persistencia escuchando en el puerto 5560.
./bin/persistence -bind "tcp://*:5560" "$@"
