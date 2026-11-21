// rmc de la maquina 2, la capa de negocio que atiende las operaciones sincronas.
// recibe solicitudes del gt, le pide a persistencia lo necesario y devuelve el resultado.
// atiende consulta, reserva y modificacion, la cancelacion no pasa por aqui porque es asincrona.

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
	gt := flag.String("gt", "tcp://localhost:5556", "endpoint DEALER del Gestor de Transacciones")
	// ESCALABILIDAD HORIZONTAL, una o varias direcciones de persistencia separadas por coma
	persistencia := flag.String("persistencia", "tcp://localhost:5560", "endpoint(s) del Servicio de Persistencia, varios separados por coma")
	id := flag.String("id", "RMC-1", "nombre de esta instancia de RMC")
	timeout := flag.Duration("timeout", 5*time.Second, "cuanto esperar a persistencia antes de rendirse")
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetPrefix(*id + " ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// aqui se crea el socket rep que recibe del gt, siempre recibe y luego responde
	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
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
	defer per.Cerrar()
	log.Printf("listo, recibe del gestor en %s y habla con persistencia en %s", *gt, *persistencia)

	for {
		mensaje, err := socket.RecvBytes(0)
		if err != nil {
			log.Printf("no se pudo recibir: %v", err)
			continue
		}
		var s model.Solicitud
		var resp model.Respuesta
		if err := json.Unmarshal(mensaje, &s); err != nil {
			resp = model.Respuesta{Mensaje: "no se pudo leer la solicitud"}
		} else {
			resp = procesar(per, s)
		}
		resp.Instancia = *id
		datos, _ := json.Marshal(resp)
		// un socket rep siempre tiene que responder
		if _, err := socket.SendBytes(datos, 0); err != nil {
			log.Printf("no se pudo responder al gestor: %v", err)
		}
	}
}

// procesar mira el tipo de solicitud y la manda a consultar o a reservar
func procesar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	switch s.Tipo {
	case model.TipoConsulta:
		return consultar(per, s)
	case model.TipoReserva:
		return reservar(per, s)
	
	case model.TipoModificacion:
		return modificar(per, s)
	}
	log.Printf("%s, %s: operacion no reconocida", s.Resumen(), model.Resultado(false))
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: fmt.Sprintf("no se reconoce la operacion %s", s.Tipo)}
}

// consultar reenvia la consulta a persistencia y devuelve los eventos del mes
func consultar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	var resp model.Respuesta
	// aqui se crea la peticion de consulta y se espera la respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpConsultarEventos, Solicitud: s}, &resp); err != nil {
		return sinPersistencia(s, err)
	}
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
	if !disponibilidad.Exito || disponibilidad.AsientosDisponibles < s.CantidadAsientos {
		// aqui se crea la respuesta de rechazo
		resp := model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: disponibilidad.Mensaje}
		if disponibilidad.Exito {
			resp.Mensaje = fmt.Sprintf("no alcanzan los asientos de %s en la fecha %d, pidio %s y %s", s.IdEvento, s.IdOcurrencia, model.Asientos(s.CantidadAsientos), model.Quedan(disponibilidad.AsientosDisponibles))
		}
		log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(false), resp.Mensaje)
		return resp
	}
	// paso 2, aqui se crea la reserva en la base a traves de persistencia
	var confirmacion model.Respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpCrearReserva, Solicitud: s}, &confirmacion); err != nil {
		return sinPersistencia(s, err)
	}
	log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(confirmacion.Exito), confirmacion.Mensaje)
	return confirmacion
}

// modificar le pide a persistencia cambiar los asientos o la fecha de una reserva en una sola operacion
// bloquea los asientos mientras se ejecuta la funcion
func modificar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	// aqui llega la respuesta de persistencia
	var resp model.Respuesta
	// aqui se crea la peticion de modificacion y se espera la respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpModificarReserva, Solicitud: s}, &resp); err != nil {
		return sinPersistencia(s, err)
	}
	// deja registro del resultado
	log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(resp.Exito), resp.Mensaje)
	return resp
}

// sinPersistencia arma el error que ve el cliente cuando persistencia no contesto
func sinPersistencia(s model.Solicitud, err error) model.Respuesta {
	log.Printf("%s, %s: persistencia no contesto (%v)", s.Resumen(), model.Resultado(false), err)
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "persistencia no contesto, intente otra vez"}
}
