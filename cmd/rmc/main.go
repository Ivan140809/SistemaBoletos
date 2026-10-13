// rmc (reservas, modificaciones y consultas), maquina 2 (capa de negocio).
//
// es el que atiende las operaciones sincronas. recibe solicitudes del gt, decide
// que hay que pedirle a persistencia y devuelve el resultado.
//
//	gt --(dealer/rep)--> [ rmc ] --(req/rep)--> persistencia (m3)
//
// por ahora solo hace consulta y reserva. la modificacion de reservas no esta
// hecha, no era de la primera entrega.
//
// detalle importante de como esta armado: rmc hace connect al gt, no
// bind. como el gt es el que se queda esperando, se pueden levantar varios rmc
// (cada uno con su -id) apuntando al mismo gt y el dealer les va repartiendo las
// solicitudes por turnos. de ahi sale la escalabilidad de la segunda entrega, sin
// tocar el gt.
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

func main() {
	gt := flag.String("gt", "tcp://localhost:5556", "endpoint DEALER del Gestor de Transacciones")
	persistencia := flag.String("persistencia", "tcp://localhost:5560", "endpoint del Servicio de Persistencia")
	// el nombre de esta instancia va en cada respuesta (campo instancia), asi se
	// ve cual rmc atendio cada solicitud cuando hay varios.
	id := flag.String("id", "RMC-1", "nombre de esta instancia de RMC")
	timeout := flag.Duration("timeout", 5*time.Second, "cuanto esperar a persistencia antes de rendirse")
	flag.Parse()

	// el prefijo del log es el nombre de la instancia (rmc-1, rmc-2...).
	log.SetOutput(os.Stdout)
	log.SetPrefix(*id + " ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// rep es el lado servidor de request/reply. la regla es estricta: recibir,
	// responder, recibir, responder. no puede recibir dos seguidos sin contestar
	// el primero, por eso en el ciclo siempre se responde, hasta si hay error.
	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	defer socket.Close()
	// connect y no bind: rmc busca al gt. si el gt todavia no esta arriba no pasa
	// nada, zeromq reintenta solo en segundo plano.
	if err := socket.Connect(*gt); err != nil { // rmc se conecta al gestor, asi se pueden sumar mas instancias sin tocar nada
		log.Fatalf("no se pudo conectar al GT: %v", err)
	}
	// "per" es el canal hacia persistencia: un socket req envuelto en nuestro
	// clientereq (internal/communication/req.go).
	per, err := communication.NuevoClienteReq(*persistencia, *timeout)
	if err != nil {
		log.Fatal(err)
	}
	defer per.Cerrar()
	log.Printf("listo, recibe del gestor en %s y habla con persistencia en %s", *gt, *persistencia)

	// ciclo principal, atiende de a una solicitud (rmc es de un solo hilo).
	for {
		// se bloquea hasta que llegue algo. el rep ya le quito la identidad y el
		// frame vacio, asi que aqui llega solo el cuerpo.
		mensaje, err := socket.RecvBytes(0)
		if err != nil {
			log.Printf("no se pudo recibir: %v", err)
			continue
		}
		var s model.Solicitud
		var resp model.Respuesta
		// si el json viene roto se arma una respuesta de error, si no se procesa.
		if err := json.Unmarshal(mensaje, &s); err != nil {
			resp = model.Respuesta{Mensaje: "no se pudo leer la solicitud"}
		} else {
			resp = procesar(per, s)
		}
		// se marca la respuesta con el nombre de esta instancia.
		resp.Instancia = *id
		datos, _ := json.Marshal(resp)
		// responder es obligatorio en un rep, si no el socket queda bloqueado.
		if _, err := socket.SendBytes(datos, 0); err != nil {
			log.Printf("no se pudo responder al gestor: %v", err)
		}
	}
}

// procesar mira el tipo de solicitud y la manda a consultar o a reservar.
func procesar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	// en go cada case termina solo, no hace falta break.
	switch s.Tipo {
	case model.TipoConsulta:
		return consultar(per, s)
	case model.TipoReserva:
		return reservar(per, s)
	}
	// si llega aqui el tipo no se conoce. el gt ya lo habria frenado en
	// validar, esto es por si acaso.
	log.Printf("%s, %s: operacion no reconocida", s.Resumen(), model.Resultado(false))
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: fmt.Sprintf("no se reconoce la operacion %s", s.Tipo)}
}

// consultar no calcula nada, solo reenvia la consulta a persistencia y devuelve
// lo que conteste (los eventos del mes con sus asientos). es el flujo del
// enunciado: recibir, mandar a persistencia, esperar y devolver.
func consultar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	var resp model.Respuesta
	// pedir manda la peticion y espera la respuesta, que se escribe en resp
	// (por eso se pasa &resp). peticiondb junta la operacion que queremos y la
	// solicitud original.
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpConsultarEventos, Solicitud: s}, &resp); err != nil {
		// normalmente es que persistencia no contesto a tiempo o esta caida.
		return sinPersistencia(s, err)
	}
	log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(resp.Exito), resp.Mensaje)
	return resp
}

// reservar sigue el flujo del enunciado, con dos viajes a persistencia:
//
//	1) preguntar cuantos asientos libres hay        (opconsultardisponibilidad)
//	2) si alcanzan, pedir que se guarde la reserva  (opcrearreserva)
//
// ojo, pregunta tipica de sustentacion: entre el paso 1 y el 2 otro cliente se
// puede haber llevado los asientos. por eso el paso 1 es solo una revision
// rapida y la garantia real esta en el paso 2: la funcion sql crear_reserva resta
// con un update condicionado (asientos_disponibles >= cantidad) que es atomico en
// la base. si ya no alcanzan, la base rechaza.
func reservar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	// paso 1: disponibilidad.
	var disponibilidad model.Respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpConsultarDisponibilidad, Solicitud: s}, &disponibilidad); err != nil {
		return sinPersistencia(s, err)
	}
	// se rechaza si la consulta fallo (por ejemplo el evento no existe) o si piden
	// mas de lo que queda. aqui no se toca la base.
	if !disponibilidad.Exito || disponibilidad.AsientosDisponibles < s.CantidadAsientos {
		resp := model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: disponibilidad.Mensaje}
		if disponibilidad.Exito {
			// la consulta salio bien pero no alcanzan, se explica cuantos quedan.
			resp.Mensaje = fmt.Sprintf("no alcanzan los asientos de %s en la fecha %d, pidio %s y %s", s.IdEvento, s.IdOcurrencia, model.Asientos(s.CantidadAsientos), model.Quedan(disponibilidad.AsientosDisponibles))
		}
		log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(false), resp.Mensaje)
		return resp
	}
	// paso 2: confirmar la reserva en la base (a traves de persistencia).
	var confirmacion model.Respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpCrearReserva, Solicitud: s}, &confirmacion); err != nil {
		return sinPersistencia(s, err)
	}
	log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(confirmacion.Exito), confirmacion.Mensaje)
	return confirmacion
}

// sinpersistencia arma la respuesta de error para cuando persistencia no
// contesto (timeout o caida). este es el mensaje que ve el cliente.
func sinPersistencia(s model.Solicitud, err error) model.Respuesta {
	log.Printf("%s, %s: persistencia no contesto (%v)", s.Resumen(), model.Resultado(false), err)
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "persistencia no contesto, intente otra vez"}
}
