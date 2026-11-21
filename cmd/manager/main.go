// gestor de transacciones gt de la maquina 2, la puerta de entrada de todos los clientes.
// no hace logica de negocio, solo valida que la solicitud venga bien armada y la enruta a rmc.
// abre tres sockets con bind y los demas procesos se conectan a el.
// frontend router en el puerto 5555 para los clientes.
// backend dealer en el puerto 5556 que reparte el trabajo entre las instancias de rmc.
// cancelaciones push en el puerto 5557, las cancelaciones se le pasan al servicio de cancelaciones
// y el gt le responde al cliente de una vez sin esperar a que se liberen los asientos.

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
	"fmt"
)

// main arranca el gt y atiende mensajes para siempre
func main() {
	dirClientes := flag.String("clientes", "tcp://*:5555", "endpoint donde se conectan los clientes")
	dirRMC := flag.String("rmc", "tcp://*:5556", "endpoint donde se conectan las instancias de RMC")
	dirCancelaciones := flag.String("cancelaciones", "tcp://*:5557", "endpoint donde se conecta el servicio de cancelaciones")
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetPrefix("GT ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// aqui se crea el router que atiende a los clientes
	frontend := abrir(zmq.ROUTER, *dirClientes)
	// ESCALABILIDAD HORIZONTAL, aqui se conectan todos los rmc que se lancen
	backend := abrir(zmq.DEALER, *dirRMC)
	// aqui se crea el push para las cancelaciones
	cancelaciones := abrir(zmq.PUSH, *dirCancelaciones)
	defer cancelaciones.Close()
	log.Printf("listo, los clientes entran por %s, rmc por %s y cancelaciones por %s", *dirClientes, *dirRMC, *dirCancelaciones)

	// aqui se crea el poller que espera en varios sockets a la vez con un solo hilo
	poller := zmq.NewPoller()
	poller.Add(frontend, zmq.POLLIN)
	poller.Add(backend, zmq.POLLIN)

	for {
		listos, err := poller.Poll(-1)
		if err != nil {
			log.Printf("fallo el poller: %v", err)
			continue
		}
		for _, listo := range listos {
			switch listo.Socket {
			case frontend:
				desdeCliente(frontend, backend,cancelaciones)
			case backend:
				desdeRMC(frontend, backend)
			}
		}
	}
}

// abrir crea un socket del tipo pedido y lo deja escuchando en el endpoint
func abrir(tipo zmq.Type, endpoint string) *zmq.Socket {
	// aqui se crea el socket
	socket, err := zmq.NewSocket(tipo)
	if err != nil {
		log.Fatal(err)
	}
	// bind deja el socket esperando a que los demas se conecten
	if err := socket.Bind(endpoint); err != nil {
		log.Fatalf("no se pudo abrir %s: %v", endpoint, err)
	}
	return socket
}

// desdeCliente procesa una solicitud de un cliente, llega en tres partes identidad vacio y json
func desdeCliente(frontend, backend *zmq.Socket, cancelaciones *zmq.Socket) {
	partes, err := frontend.RecvMessage(0)
	if err != nil || len(partes) < 3 {
		return
	}
	// la identidad dice a quien responder y el cuerpo es la solicitud
	identidad, cuerpo := partes[0], partes[len(partes)-1]

	// aqui se crea la solicitud a partir del json
	var s model.Solicitud
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
	// cancelacion es asincrona, va por el canal push y el cliente no espera el resultado se le envia posteriormente
	if s.Tipo == model.TipoCancelacion {
		cancelar(frontend, cancelaciones, identidad,s, cuerpo )
		return
	}
	// ESCALABILIDAD HORIZONTAL, reenvia a algun rmc con la identidad para saber a quien devolver
	if _, err := backend.SendMessageDontwait(identidad, "", cuerpo); err != nil {
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "no hay ningun rmc conectado para atenderla"})
		return
	}
	// el gt no espera la respuesta aqui, llega despues por el backend en desdeRMC
}

// cancelar pasa la cancelacion al servicio de cancelaciones y le responde al cliente de una vez.
// el resultado real le llega despues al cliente como notificacion en su direccion
func cancelar(frontend, cancelaciones *zmq.Socket, identidad string, s model.Solicitud, cuerpo string) {
	// sin bloquear, si el servicio de cancelaciones no esta conectado falla de una vez
	if _, err := cancelaciones.SendBytes([]byte(cuerpo), zmq.DONTWAIT); err != nil {
		log.Printf("%s, %s: no hay servicio de cancelaciones conectado", s.Resumen(), model.Resultado(false))
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "no hay servicio de cancelaciones conectado para atenderla"})
		return
	}
	// deja registro de que se acepto
	log.Printf("%s, se acepta y se pasa a cancelaciones", s.Resumen())
	// confirma la recepcion, exito aqui quiere decir recibida y no cancelada todavia
	responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Exito: true, Mensaje: fmt.Sprintf("cancelacion de %s recibida, el resultado le llega como notificacion a %s", s.IdReserva, s.DireccionNotificacion)})
}

// desdeRMC recibe la respuesta de un rmc y se la devuelve al cliente
func desdeRMC(frontend, backend *zmq.Socket) {
	// recibe identidad del cliente, vacio y respuesta en json
	partes, err := backend.RecvMessage(0)
	if err != nil || len(partes) < 3 {
		return
	}
	var r model.Respuesta
	json.Unmarshal([]byte(partes[len(partes)-1]), &r)
	log.Printf("%s contesto la solicitud %s, %s: %s", r.Instancia, r.IdSolicitud, model.Resultado(r.Exito), r.Mensaje)
	// el router usa la identidad para entregarla al cliente correcto
	frontend.SendMessage(partes)
}

// responder le contesta al cliente directo desde el gt sin pasar por rmc
func responder(frontend *zmq.Socket, identidad string, r model.Respuesta) {
	datos, _ := json.Marshal(r)
	frontend.SendMessage(identidad, "", string(datos))
}
