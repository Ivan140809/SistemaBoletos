// gestor de transacciones gt de la maquina 2, la puerta de entrada de todos los clientes.
// no hace logica de negocio, solo valida que la solicitud venga bien armada y la enruta a rmc.
// abre tres sockets con bind y los demas procesos se conectan a el.
// frontend router en el puerto 5555 para los clientes.
// backend dealer en el puerto 5556 que reparte el trabajo entre las instancias de rmc.
// cancelaciones push en el puerto 5557 para mandar cancelaciones en la segunda entrega.
//
// ESCALABILIDAD HORIZONTAL, el backend es donde escala la capa de negocio, el dealer reparte
// por turnos entre todos los rmc conectados sin importar la maquina y sin reiniciar el gt.
package main

import (
	"encoding/json"
	"flag"
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"sistemaboletos/internal/model"
)

// main arranca el gt y atiende mensajes para siempre
func main() {
	// direccion donde se conectan los clientes
	dirClientes := flag.String("clientes", "tcp://*:5555", "endpoint donde se conectan los clientes")
	// direccion donde se conectan los rmc
	dirRMC := flag.String("rmc", "tcp://*:5556", "endpoint donde se conectan las instancias de RMC")
	// direccion donde se conecta el servicio de cancelaciones
	dirCancelaciones := flag.String("cancelaciones", "tcp://*:5557", "endpoint donde se conecta el servicio de cancelaciones")
	// lee todas las opciones que se pasaron
	flag.Parse()

	// los mensajes salen por la terminal
	log.SetOutput(os.Stdout)
	// cada mensaje empieza con GT
	log.SetPrefix("GT ")
	// cada mensaje lleva la hora con microsegundos
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// aqui se crea el router que atiende a los clientes
	frontend := abrir(zmq.ROUTER, *dirClientes)
	// ESCALABILIDAD HORIZONTAL, aqui se conectan todos los rmc que se lancen
	backend := abrir(zmq.DEALER, *dirRMC)
	// aqui se crea el push para las cancelaciones
	cancelaciones := abrir(zmq.PUSH, *dirCancelaciones)
	// cierra cancelaciones al terminar
	defer cancelaciones.Close()
	// avisa que el gt ya esta listo
	log.Printf("listo, los clientes entran por %s, rmc por %s y cancelaciones por %s", *dirClientes, *dirRMC, *dirCancelaciones)

	// aqui se crea el poller que espera en varios sockets a la vez con un solo hilo
	poller := zmq.NewPoller()
	// vigila lo que llega de los clientes
	poller.Add(frontend, zmq.POLLIN)
	// vigila lo que llega de los rmc
	poller.Add(backend, zmq.POLLIN)

	// ciclo infinito
	for {
		// espera hasta que llegue algo de un cliente o de un rmc
		listos, err := poller.Poll(-1)
		// si falla vuelve a esperar
		if err != nil {
			log.Printf("fallo el poller: %v", err)
			continue
		}
		// recorre los sockets que tienen algo
		for _, listo := range listos {
			// mira cual socket desperto
			switch listo.Socket {
			case frontend:
				// llego algo de un cliente y se reenvia a rmc
				desdeCliente(frontend, backend)
			case backend:
				// llego la respuesta de un rmc y se devuelve al cliente
				desdeRMC(frontend, backend)
			}
		}
	}
}

// abrir crea un socket del tipo pedido y lo deja escuchando en el endpoint
func abrir(tipo zmq.Type, endpoint string) *zmq.Socket {
	// aqui se crea el socket
	socket, err := zmq.NewSocket(tipo)
	// si falla se detiene el programa
	if err != nil {
		log.Fatal(err)
	}
	// bind deja el socket esperando a que los demas se conecten
	if err := socket.Bind(endpoint); err != nil {
		log.Fatalf("no se pudo abrir %s: %v", endpoint, err)
	}
	// devuelve el socket listo
	return socket
}

// desdeCliente procesa una solicitud de un cliente, llega en tres partes identidad vacio y json
func desdeCliente(frontend, backend *zmq.Socket) {
	// recibe el mensaje completo del cliente
	partes, err := frontend.RecvMessage(0)
	// si fallo o no trae las tres partes se ignora
	if err != nil || len(partes) < 3 {
		return
	}
	// la identidad dice a quien responder y el cuerpo es la solicitud
	identidad, cuerpo := partes[0], partes[len(partes)-1]

	// aqui se crea la solicitud a partir del json
	var s model.Solicitud
	// si el json viene roto se responde de una vez sin molestar a rmc
	if err := json.Unmarshal([]byte(cuerpo), &s); err != nil {
		responder(frontend, identidad, model.Respuesta{Mensaje: "no se pudo leer la solicitud"})
		return
	}
	// valida la solicitud y si viene mal armada no llega a rmc ni a la base
	if err := s.Validar(); err != nil {
		log.Printf("%s, %s: se rechaza porque %v", s.Resumen(), model.Resultado(false), err)
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "solicitud rechazada, " + err.Error()})
		return
	}
	// ESCALABILIDAD HORIZONTAL, reenvia a algun rmc con la identidad para saber a quien devolver
	// sin bloquear, si no hay ningun rmc conectado falla de una vez
	if _, err := backend.SendMessageDontwait(identidad, "", cuerpo); err != nil {
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "no hay ningun rmc conectado para atenderla"})
		return
	}
	// no espera la respuesta, llega despues por el backend en desdeRMC
}

// desdeRMC recibe la respuesta de un rmc y se la devuelve al cliente
func desdeRMC(frontend, backend *zmq.Socket) {
	// recibe identidad del cliente, vacio y respuesta en json
	partes, err := backend.RecvMessage(0)
	// si fallo o no trae las tres partes se ignora
	if err != nil || len(partes) < 3 {
		return
	}
	// se lee la respuesta solo para el log
	var r model.Respuesta
	json.Unmarshal([]byte(partes[len(partes)-1]), &r)
	// deja registro de que rmc contesto y con que resultado
	log.Printf("%s contesto la solicitud %s, %s: %s", r.Instancia, r.IdSolicitud, model.Resultado(r.Exito), r.Mensaje)
	// el router usa la identidad para entregarla al cliente correcto
	frontend.SendMessage(partes)
}

// responder le contesta al cliente directo desde el gt sin pasar por rmc
func responder(frontend *zmq.Socket, identidad string, r model.Respuesta) {
	// convierte la respuesta a json
	datos, _ := json.Marshal(r)
	// manda identidad, vacio y respuesta
	frontend.SendMessage(identidad, "", string(datos))
}
