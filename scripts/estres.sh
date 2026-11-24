#!/usr/bin/env bashd
set -uo pipefail
cd "$(dirname "$0")/.."

GT="${GT:-${SRB_GT:-tcp://${IP_NEGOCIO:-localhost}:5555}}"
MES="${MES:-2026-11}"
CLIENTES="${CLIENTES:-10}"
RONDAS="${RONDAS:-20}"
# eventos entre los que se reparten las reservas, deben existir con fecha 1
EVENTOS="${EVENTOS:-E1 E2 E3}"
# porcentaje de reservas que se cancelan al final de la ronda
CANCELAR="${CANCELAR:-50}"
CLIENTE=./bin/client

[ -x "$CLIENTE" ] || { echo "falta $CLIENTE, compile con go build -o bin/client ./cmd/client"; exit 1; }
read -ra LISTA_EVENTOS <<< "$EVENTOS"

# aqui se crea la carpeta con el log de cada cliente y el archivo de resultados que comparten
SALIDA="${SALIDA:-metrics/estres-$(date +%Y%m%d-%H%M%S)}"
RESULTADOS="$SALIDA/resultados.txt"
if ! mkdir -p "$SALIDA" || ! : > "$RESULTADOS"; then
	echo "no se puede escribir en $SALIDA, si metrics es de root use sudo chown -R $USER metrics o SALIDA=otra_carpeta"
	exit 1
fi

# antes de empezar se revisa que el gt conteste, si no no tiene sentido seguir
if ! "$CLIENTE" consultar -gt "$GT" -id Estres -mes "$MES" > /dev/null; then
	echo "el gt en $GT no contesta, revise que la m2 y la m3 esten arriba"
	exit 1
fi

# operar corre un comando del cliente y anota en resultados la operacion y como termino.
# exito si el sistema la hizo, rechazo si contesto que no se podia y error si no hubo respuesta
operar() {
	local nombre=$1 operacion=$2 salida estado
	shift 2
	salida=$("$CLIENTE" "$operacion" -gt "$GT" -id "$nombre" "$@" 2>&1)
	if [ $? -ne 0 ]; then
		estado=error
	elif grep -q ", fallo: " <<< "$salida"; then
		estado=rechazo
	else
		estado=exito
	fi
	echo "$operacion $estado" >> "$RESULTADOS"
	echo "[$estado] $salida" >> "$SALIDA/$nombre.log"
	printf '%s' "$salida"
}

# un_cliente es lo que hace cada cliente durante la prueba
un_cliente() {
	local nombre=$1 ronda evento salida reserva
	for ((ronda = 1; ronda <= RONDAS; ronda++)); do
		operar "$nombre" consultar -mes "$MES" > /dev/null
		evento=${LISTA_EVENTOS[RANDOM % ${#LISTA_EVENTOS[@]}]}
		salida=$(operar "$nombre" reservar -evento "$evento" -ocurrencia 1 -asientos $((RANDOM % 2 + 1)))
		# sin reserva no hay nada que modificar ni cancelar en esta ronda
		reserva=$(grep -oE 'reserva R[0-9]+ lista' <<< "$salida" | grep -oE 'R[0-9]+')
		[ -n "$reserva" ] || continue
		operar "$nombre" modificar -reserva "$reserva" -asientos $((RANDOM % 3 + 1)) > /dev/null
		if ((RANDOM % 100 < CANCELAR)); then
			operar "$nombre" cancelar -reserva "$reserva" -espera 10s > /dev/null
		fi
	done
}

echo "estres contra $GT: $CLIENTES clientes, $RONDAS rondas cada uno, eventos $EVENTOS, logs en $SALIDA"
inicio=$(date +%s)
# aqui se lanzan todos los clientes en paralelo
for ((i = 1; i <= CLIENTES; i++)); do
	un_cliente "Estres$i" &
done
wait
duracion=$(($(date +%s) - inicio))

# resumen por operacion
echo
printf '%-12s %8s %8s %8s\n' operacion exito rechazo error
for op in consultar reservar modificar cancelar; do
	printf '%-12s %8d %8d %8d\n' "$op" \
		"$(grep -c "^$op exito" "$RESULTADOS")" \
		"$(grep -c "^$op rechazo" "$RESULTADOS")" \
		"$(grep -c "^$op error" "$RESULTADOS")"
done
total=$(wc -l < "$RESULTADOS")
errores=$(grep -c " error$" "$RESULTADOS")
echo "$total solicitudes en ${duracion}s"

# consulta final, si el sistema se cayo durante la prueba aqui se nota
echo
if "$CLIENTE" consultar -gt "$GT" -id Estres -mes "$MES"; then
	echo "el sistema sigue contestando despues de la prueba"
else
	echo "el sistema NO contesta despues de la prueba"
	exit 1
fi
if [ "$errores" -gt 0 ]; then
	echo "hubo $errores solicitudes sin respuesta, busque [error] en $SALIDA/*.log"
	exit 1
fi
