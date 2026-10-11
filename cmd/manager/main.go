package main

import (
	"encoding/json"
	"flag"
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"sistemaboletos/internal/model"
)

func main() {
	dirClientes := flag.String("clientes", "tcp://*:5555", "endpoint donde se conectan los clientes")
	dirRMC := flag.String("rmc", "tcp://*:5556", "endpoint donde se conectan las instancias de RMC")
	dirCancelaciones := flag.String("cancelaciones", "tcp://*:5557", "endpoint donde se conecta el servicio de cancelaciones")
	flag.Parse()
	log.SetOutput(os.Stdout)
	log.SetPrefix("GT ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	frontend := abrir(zmq.ROUTER, *dirClientes)
	backend := abrir(zmq.DEALER, *dirRMC)
	cancelaciones := abrir(zmq.PUSH, *dirCancelaciones)
	defer cancelaciones.Close()
	log.Printf("clientes en %s / RMC en %s / cancelaciones en %s", *dirClientes, *dirRMC, *dirCancelaciones)

	poller := zmq.NewPoller()
	poller.Add(frontend, zmq.POLLIN)
	poller.Add(backend, zmq.POLLIN)
	for {
		listos, err := poller.Poll(-1) // espera hasta que llegue algo de un cliente o de RMC
		if err != nil {
			log.Printf("error en poll %v", err)
			continue
		}
		for _, listo := range listos {
			switch listo.Socket {
			case frontend:
				desdeCliente(frontend, backend)
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
		log.Fatalf("no se pudo abrir %s %v", endpoint, err)
	}
	return socket
}

func desdeCliente(frontend, backend *zmq.Socket) {
	partes, err := frontend.RecvMessage(0)
	if err != nil || len(partes) < 3 {
		return
	}
	identidad, cuerpo := partes[0], partes[len(partes)-1]
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
	if _, err := backend.SendMessageDontwait(identidad, "", cuerpo); err != nil {
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "no hay instancias de RMC disponibles"})
		return
	}
}

func desdeRMC(frontend, backend *zmq.Socket) {
	partes, err := backend.RecvMessage(0)
	if err != nil || len(partes) < 3 {
		return
	}
	var r model.Respuesta
	json.Unmarshal([]byte(partes[len(partes)-1]), &r)
	log.Printf("respuesta de %s sol=%s -> %s %s", r.Instancia, r.IdSolicitud, model.Resultado(r.Exito), r.Mensaje)
	frontend.SendMessage(partes)
}

func responder(frontend *zmq.Socket, identidad string, r model.Respuesta) {
	datos, _ := json.Marshal(r)
	frontend.SendMessage(identidad, "", string(datos))
}
