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
	id := flag.String("id", "RMC-1", "nombre de esta instancia de RMC")
	timeout := flag.Duration("timeout", 5*time.Second, "tiempo máximo de espera a Persistencia")
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetPrefix(*id + " ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	defer socket.Close()
	if err := socket.Connect(*gt); err != nil { // RMC se conecta al GT; en la 2da entrega basta lanzar más instancias
		log.Fatalf("no se pudo conectar al GT: %v", err)
	}
	per, err := communication.NuevoClienteReq(*persistencia, *timeout)
	if err != nil {
		log.Fatal(err)
	}
	defer per.Cerrar()
	log.Printf("atendiendo solicitudes del GT en %s | Persistencia en %s", *gt, *persistencia)

	for {
		mensaje, err := socket.RecvBytes(0)
		if err != nil {
			log.Printf("error recibiendo: %v", err)
			continue
		}
		var s model.Solicitud
		var resp model.Respuesta
		if err := json.Unmarshal(mensaje, &s); err != nil {
			resp = model.Respuesta{Mensaje: "solicitud mal formada"}
		} else {
			resp = procesar(per, s)
		}
		resp.Instancia = *id
		datos, _ := json.Marshal(resp)
		if _, err := socket.SendBytes(datos, 0); err != nil {
			log.Printf("error respondiendo al GT: %v", err)
		}
	}
}

func procesar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	switch s.Tipo {
	case model.TipoConsulta:
		return consultar(per, s)
	case model.TipoReserva:
		return reservar(per, s)
	}
	log.Printf("%s -> no disponible en la arquitectura base", s.Resumen())
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: fmt.Sprintf("la operación %s no está disponible en la arquitectura base", s.Tipo)}
}

func consultar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	var resp model.Respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpConsultarEventos, Solicitud: s}, &resp); err != nil {
		return sinPersistencia(s, err)
	}
	log.Printf("%s -> %s %s", s.Resumen(), model.Resultado(resp.Exito), resp.Mensaje)
	return resp
}

func reservar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	var disponibilidad model.Respuesta
	// paso 2 del enunciado: consultar la disponibilidad en tiempo real
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpConsultarDisponibilidad, Solicitud: s}, &disponibilidad); err != nil {
		return sinPersistencia(s, err)
	}
	if !disponibilidad.Exito || disponibilidad.AsientosDisponibles < s.CantidadAsientos {
		resp := model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: disponibilidad.Mensaje}
		if disponibilidad.Exito {
			resp.Mensaje = fmt.Sprintf("no hay asientos suficientes en %s/%d: pidió %d, quedan %d", s.IdEvento, s.IdOcurrencia, s.CantidadAsientos, disponibilidad.AsientosDisponibles)
		}
		log.Printf("%s -> %s %s", s.Resumen(), model.Resultado(false), resp.Mensaje)
		return resp
	}
	var confirmacion model.Respuesta
	// paso 3: confirmar en la BD; Persistencia vuelve a verificar y guarda en una sola transacción
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpCrearReserva, Solicitud: s}, &confirmacion); err != nil {
		return sinPersistencia(s, err)
	}
	log.Printf("%s -> %s %s", s.Resumen(), model.Resultado(confirmacion.Exito), confirmacion.Mensaje)
	return confirmacion
}

func sinPersistencia(s model.Solicitud, err error) model.Respuesta {
	log.Printf("%s -> Persistencia no responde: %v", s.Resumen(), err)
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "el servicio de persistencia no responde"}
}
