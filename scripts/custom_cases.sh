#!/usr/bin/env bash
# ESCENARIO DE DEMOSTRACIÓN CON CASOS BORDE (se ejecuta en la M1, el cliente).
# Prueba cosas que deben FALLAR bien: fecha inexistente, evento inexistente,
# solicitudes mal formadas (las frena el GT), evento agotado y competencia
# concurrente por los últimos asientos.
# Requisito: M3 arrancada con OTRO caso de prueba:
#   ./scripts/machine3_storage.sh -inicial data/custom_case.json
# Uso:   IP_NEGOCIO=<IP_de_M2> ./scripts/custom_cases.sh
set -euo pipefail
cd "$(dirname "$0")/.."
: "${IP_NEGOCIO:?falta IP_NEGOCIO, la ip de la maquina 2}"
# Dirección del Gestor de Transacciones (puerto 5555 de la M2).
GT="tcp://${IP_NEGOCIO}:5555"

# Función auxiliar: imprime el texto del paso ($1) y pausa hasta que se presione ENTER.
paso() {
	echo "$1"
	read -rp "enter para seguir"
}

echo "antes de empezar, la maquina 3 debe estar corriendo con: ./scripts/machine3_storage.sh -inicial data/custom_case.json"

paso "1. eventos de noviembre, salen E1 con dos fechas, E2, E3 y el taller E4"
./bin/client consultar -gt "$GT" -id Laura -mes 2026-11

paso "2. eventos de diciembre, solo sale la feria E5"
./bin/client consultar -gt "$GT" -id Laura -mes 2026-12

paso "3. Laura reserva 4 asientos para la segunda fecha del concierto E1"
./bin/client reservar -gt "$GT" -id Laura -evento E1 -ocurrencia 2 -asientos 4

paso "4. Andres pide la fecha 2 de la obra E2, pero la obra solo tiene una fecha"
./bin/client reservar -gt "$GT" -id Andres -evento E2 -ocurrencia 2 -asientos 2

paso "5. Andres pide un evento que no existe, E9"
./bin/client reservar -gt "$GT" -id Andres -evento E9 -asientos 2

paso "6. Andres pide 0 asientos, el gestor lo frena antes de llegar a rmc"
./bin/client reservar -gt "$GT" -id Andres -evento E2 -asientos 0

paso "7. alguien consulta escribiendo el mes en letras, el gestor tambien lo frena"
./bin/client consultar -gt "$GT" -id Andres -mes noviembre

paso "8. Sofia compra los 10 cupos del taller E4 y lo deja agotado"
./bin/client reservar -gt "$GT" -id Sofia -evento E4 -asientos 10

paso "9. Camilo llega tarde y pide 1 cupo del taller E4"
./bin/client reservar -gt "$GT" -id Camilo -evento E4 -asientos 1

paso "10. ocho hinchas piden 20 boletas cada uno para la final E3, solo alcanzan para cinco"
./bin/client concurrente -gt "$GT" -id Hincha -evento E3 -asientos 20,20,20,20,20,20,20,20

paso "11. Pedro compra 99 boletas de la obra E2 y deja un solo asiento libre"
./bin/client reservar -gt "$GT" -id Pedro -evento E2 -asientos 99

paso "12. tres personas pelean por ese ultimo asiento de la obra E2, solo una lo consigue"
./bin/client concurrente -gt "$GT" -id Rezagado -evento E2 -asientos 1,1,1

paso "13. asi queda noviembre al final"
./bin/client consultar -gt "$GT" -id Laura -mes 2026-11
