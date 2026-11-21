// rmc de la maquina 2, la capa de negocio que atiende las operaciones sincronas.
// recibe solicitudes del gt, le pide a persistencia lo necesario y devuelve el resultado,
// por ahora solo hace consulta y reserva porque la modificacion no era de la primera entrega.
//
// ESCALABILIDAD HORIZONTAL, rmc hace connect al gt y no bind, asi se pueden levantar varios rmc
// con su -id en esta u otras maquinas y el dealer del gt les reparte las solicitudes por turnos.
// rmc no guarda estado porque todo esta en la base, asi que agregar capacidad es lanzar otro rmc.
// ESCALABILIDAD HORIZONTAL, hacia abajo -persistencia acepta varias direcciones separadas por coma
// y el req reparte las peticiones entre varias instancias de persistencia.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"sistemaboletos/internal/communication"
	"sistemaboletos/internal/model"
	"time"
)

// main arranca el rmc y atiende solicitudes para siempre
func main() {
	// direccion del gt al que se conecta
	gt := flag.String("gt", "tcp://localhost:5556", "endpoint DEALER del Gestor de Transacciones")
	// ESCALABILIDAD HORIZONTAL, una o varias direcciones de persistencia separadas por coma
	persistencia := flag.String("persistencia", "tcp://localhost:5560", "endpoint(s) del Servicio de Persistencia, varios separados por coma")
	// nombre de esta instancia, sale en cada respuesta para saber cual rmc atendio
	id := flag.String("id", "RMC-1", "nombre de esta instancia de RMC")
	// cuanto esperar a persistencia antes de rendirse
	timeout := flag.Duration("timeout", 5*time.Second, "cuanto esperar a persistencia antes de rendirse")
	// lee todas las opciones que se pasaron
	flag.Parse()

	// los mensajes salen por la terminal
	log.SetOutput(os.Stdout)
	// cada mensaje empieza con el nombre de la instancia
	log.SetPrefix(*id + " ")
	// cada mensaje lleva la hora con microsegundos
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// aqui se crea el socket rep que recibe del gt, siempre recibe y luego responde
	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	// cierra el socket al terminar
	defer socket.Close()
	// ESCALABILIDAD HORIZONTAL, connect y no bind para poder sumar mas rmc sin tocar el gt
	if err := socket.Connect(*gt); err != nil {
		log.Fatalf("no se pudo conectar al GT: %v", err)
	}
	// aqui se crea el cliente req hacia persistencia
	per, err := communication.NuevoClienteReq(*persistencia, *timeout)
	if err != nil {
		log.Fatal(err)
	}
	// cierra el cliente al terminar
	defer per.Cerrar()
	// avisa que el rmc ya esta listo
	log.Printf("listo, recibe del gestor en %s y habla con persistencia en %s", *gt, *persistencia)

	// ciclo principal, atiende una solicitud a la vez
	for {
		// espera hasta que llegue una solicitud
		mensaje, err := socket.RecvBytes(0)
		// si falla se salta y espera la siguiente
		if err != nil {
			log.Printf("no se pudo recibir: %v", err)
			continue
		}
		// solicitud que llego y respuesta que se va a devolver
		var s model.Solicitud
		var resp model.Respuesta
		// si el json viene roto responde con error, si no la procesa
		if err := json.Unmarshal(mensaje, &s); err != nil {
			resp = model.Respuesta{Mensaje: "no se pudo leer la solicitud"}
		} else {
			resp = procesar(per, s)
		}
		// marca la respuesta con el nombre de esta instancia
		resp.Instancia = *id
		// convierte la respuesta a json
		datos, _ := json.Marshal(resp)
		// un socket rep siempre tiene que responder
		if _, err := socket.SendBytes(datos, 0); err != nil {
			log.Printf("no se pudo responder al gestor: %v", err)
		}
	}
}

// procesar mira el tipo de solicitud y la manda a consultar o a reservar
func procesar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	// segun el tipo llama a la funcion que toca
	switch s.Tipo {
	case model.TipoConsulta:
		return consultar(per, s)
	case model.TipoReserva:
		return reservar(per, s)
	}
	// tipo desconocido, el gt ya lo habria frenado pero es por si acaso
	log.Printf("%s, %s: operacion no reconocida", s.Resumen(), model.Resultado(false))
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: fmt.Sprintf("no se reconoce la operacion %s", s.Tipo)}
}

// consultar reenvia la consulta a persistencia y devuelve los eventos del mes
func consultar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	// aqui llega la respuesta de persistencia
	var resp model.Respuesta
	// aqui se crea la peticion de consulta y se espera la respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpConsultarEventos, Solicitud: s}, &resp); err != nil {
		// persistencia no contesto a tiempo o esta caida
		return sinPersistencia(s, err)
	}
	// deja registro del resultado
	log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(resp.Exito), resp.Mensaje)
	return resp
}

// reservar primero revisa los asientos libres y si alcanzan pide guardar la reserva.
// la garantia real esta en crear_reserva de la base, que es atomica aunque otro se adelante
func reservar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	// paso 1, pregunta cuantos asientos libres hay
	var disponibilidad model.Respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpConsultarDisponibilidad, Solicitud: s}, &disponibilidad); err != nil {
		return sinPersistencia(s, err)
	}
	// se rechaza si la consulta fallo o si piden mas de lo que queda
	if !disponibilidad.Exito || disponibilidad.AsientosDisponibles < s.CantidadAsientos {
		// aqui se crea la respuesta de rechazo
		resp := model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: disponibilidad.Mensaje}
		// si la consulta salio bien pero no alcanzan, se dice cuantos quedan
		if disponibilidad.Exito {
			resp.Mensaje = fmt.Sprintf("no alcanzan los asientos de %s en la fecha %d, pidio %s y %s", s.IdEvento, s.IdOcurrencia, model.Asientos(s.CantidadAsientos), model.Quedan(disponibilidad.AsientosDisponibles))
		}
		// deja registro del rechazo
		log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(false), resp.Mensaje)
		return resp
	}
	// paso 2, aqui se crea la reserva en la base a traves de persistencia
	var confirmacion model.Respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpCrearReserva, Solicitud: s}, &confirmacion); err != nil {
		return sinPersistencia(s, err)
	}
	// deja registro del resultado
	log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(confirmacion.Exito), confirmacion.Mensaje)
	return confirmacion
}

// sinPersistencia arma el error que ve el cliente cuando persistencia no contesto
func sinPersistencia(s model.Solicitud, err error) model.Respuesta {
	// deja registro del error
	log.Printf("%s, %s: persistencia no contesto (%v)", s.Resumen(), model.Resultado(false), err)
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "persistencia no contesto, intente otra vez"}
}
