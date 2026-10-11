package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"

	zmq "github.com/pebbe/zmq4"

	"sistemaboletos/internal/model"
)

func main() {
	dirClientes := flag.String("clientes", "tcp://*:5555", "endpoint ROUTER donde se conectan los clientes")
	dirRMC := flag.String("rmc", "tcp://*:5556", "endpoint DEALER donde se conectan las instancias de RMC")
	dirCancelaciones := flag.String("cancelaciones", "tcp://*:5557", "endpoint PUSH donde se conecta el servicio de cancelaciones")
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetPrefix("[GT] ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	frontend := abrir(zmq.ROUTER, *dirClientes)
	backend := abrir(zmq.DEALER, *dirRMC)
	cancelaciones := abrir(zmq.PUSH, *dirCancelaciones)
	log.Printf("clientes en %s | RMC en %s | cancelaciones en %s", *dirClientes, *dirRMC, *dirCancelaciones)

	poller := zmq.NewPoller()
	poller.Add(frontend, zmq.POLLIN)
	poller.Add(backend, zmq.POLLIN)
	for {
		listos, err := poller.Poll(-1) // espera hasta que llegue algo de un cliente o de RMC
		if err != nil {
			log.Printf("error en poll: %v", err)
			continue
		}
		for _, listo := range listos {
			switch listo.Socket {
			case frontend:
				desdeCliente(frontend, backend, cancelaciones)
			case backend:
				desdeRMC(frontend, backend)
			}
		}
	}
}

func abrir(tipo zmq.Type, endpoint string) *zmq.Socket {
	socket, err := zmq.NewSocket(tipo)
	if err != nil {
		log.Fatal(err)
	}
	if err := socket.Bind(endpoint); err != nil {
		log.Fatalf("no se pudo abrir %s: %v", endpoint, err)
	}
	return socket
}

func desdeCliente(frontend, backend, cancelaciones *zmq.Socket) {
	partes, err := frontend.RecvMessage(0)
	if err != nil || len(partes) < 3 {
		return
	}
	identidad, cuerpo := partes[0], partes[len(partes)-1] // el ROUTER entrega [identidad del cliente, "", solicitud]
	var s model.Solicitud
	if err := json.Unmarshal([]byte(cuerpo), &s); err != nil {
		responder(frontend, identidad, model.Respuesta{Mensaje: "solicitud mal formada"})
		return
	}
	if err := s.Validar(); err != nil {
		log.Printf("%s -> %s solicitud inválida: %v", s.Resumen(), model.Resultado(false), err)
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "solicitud inválida: " + err.Error()})
		return
	}
	if s.Tipo == model.TipoCancelacion {
		if _, err := cancelaciones.Send(cuerpo, zmq.DONTWAIT); err != nil {
			responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "el servicio de cancelaciones no está disponible"})
			return
		}
		log.Printf("%s -> canal asíncrono (Cancelaciones), se responde de inmediato", s.Resumen())
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Exito: true, Mensaje: "cancelación aceptada, recibirá una notificación cuando se liberen los asientos"})
		return
	}
	if _, err := backend.SendMessageDontwait(identidad, "", cuerpo); err != nil { // se conserva el sobre para saber a quién devolver la respuesta
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "no hay instancias de RMC disponibles"})
		return
	}
	log.Printf("%s -> canal síncrono (RMC)", s.Resumen())
}

func desdeRMC(frontend, backend *zmq.Socket) {
	partes, err := backend.RecvMessage(0)
	if err != nil || len(partes) < 3 {
		return
	}
	var r model.Respuesta
	json.Unmarshal([]byte(partes[len(partes)-1]), &r)
	log.Printf("respuesta de %s sol=%s -> %s %s", r.Instancia, r.IdSolicitud, model.Resultado(r.Exito), r.Mensaje)
	frontend.SendMessage(partes) // [identidad, "", respuesta]: el ROUTER la entrega al cliente correcto
}

func responder(frontend *zmq.Socket, identidad string, r model.Respuesta) {
	datos, _ := json.Marshal(r)
	frontend.SendMessage(identidad, "", string(datos))
}
