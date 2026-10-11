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
	timeout := flag.Duration("timeout", 5*time.Second, "cuanto esperar a persistencia antes de rendirse")
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetPrefix(*id + " ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	defer socket.Close()
	if err := socket.Connect(*gt); err != nil { // rmc se conecta al gestor, asi se pueden sumar mas instancias sin tocar nada
		log.Fatalf("no se pudo conectar al GT: %v", err)
	}
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
		if _, err := socket.SendBytes(datos, 0); err != nil {
			log.Printf("no se pudo responder al gestor: %v", err)
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
	log.Printf("%s, %s: operacion no reconocida", s.Resumen(), model.Resultado(false))
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: fmt.Sprintf("no se reconoce la operacion %s", s.Tipo)}
}

func consultar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	var resp model.Respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpConsultarEventos, Solicitud: s}, &resp); err != nil {
		return sinPersistencia(s, err)
	}
	log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(resp.Exito), resp.Mensaje)
	return resp
}

func reservar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	var disponibilidad model.Respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpConsultarDisponibilidad, Solicitud: s}, &disponibilidad); err != nil {
		return sinPersistencia(s, err)
	}
	if !disponibilidad.Exito || disponibilidad.AsientosDisponibles < s.CantidadAsientos {
		resp := model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: disponibilidad.Mensaje}
		if disponibilidad.Exito {
			resp.Mensaje = fmt.Sprintf("no alcanzan los asientos de %s en la fecha %d, pidio %s y %s", s.IdEvento, s.IdOcurrencia, model.Asientos(s.CantidadAsientos), model.Quedan(disponibilidad.AsientosDisponibles))
		}
		log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(false), resp.Mensaje)
		return resp
	}
	var confirmacion model.Respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpCrearReserva, Solicitud: s}, &confirmacion); err != nil {
		return sinPersistencia(s, err)
	}
	log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(confirmacion.Exito), confirmacion.Mensaje)
	return confirmacion
}

func sinPersistencia(s model.Solicitud, err error) model.Respuesta {
	log.Printf("%s, %s: persistencia no contesto (%v)", s.Resumen(), model.Resultado(false), err)
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "persistencia no contesto, intente otra vez"}
}
