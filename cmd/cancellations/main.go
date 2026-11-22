// servicio de cancelaciones de la maquina 2, atiende las cancelaciones que son asincronas.
// recibe del gt por un pull, le pide a persistencia liberar los asientos con un req y cuando
// persistencia confirma le manda la notificacion al cliente con un push a la direccion que el
// cliente dejo en la solicitud. el cliente nunca espera aqui, el gt ya le respondio que fue recibida.
//
// ESCALABILIDAD HORIZONTAL, se pueden lanzar varios servicios de cancelaciones conectados al gt
// y el push del gt les reparte las cancelaciones por turnos. hacia abajo -persistencia acepta
// varias direcciones separadas por coma igual que rmc.
package main

import (
	"encoding/json"
	"flag"
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"sistemaboletos/internal/communication"
	"sistemaboletos/internal/model"
	"time"
)

// main arranca el servicio y atiende cancelaciones para siempre
func main() {
	// direccion del push del gt
	gt := flag.String("gt", "tcp://localhost:5557", "endpoint PUSH del Gestor de Transacciones")
	// ESCALABILIDAD HORIZONTAL, una o varias direcciones de persistencia separadas por coma
	persistencia := flag.String("persistencia", "tcp://localhost:5560", "endpoints del Servicio de Persistencia, varios separados por coma")
	// cuanto esperar a persistencia antes de rendirse
	timeout := flag.Duration("timeout", 5*time.Second, "cuanto esperar a persistencia antes de rendirse")
	// cuanto intentar entregar la notificacion si el cliente tarda en recibirla
	espera := flag.Duration("espera-notificacion", 3*time.Second, "cuanto intentar entregar la notificacion al cliente")
	// lee todas las opciones que se pasaron
	flag.Parse()
	// los mensajes salen por la terminal
	log.SetOutput(os.Stdout)
	// cada mensaje empieza con cancelaciones
	log.SetPrefix("cancelaciones ")
	// cada mensaje lleva la hora con microsegundos
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// aqui se crea el socket pull que recibe del gt
	entrada, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		log.Fatal(err)
	}
	// cierra el socket al terminar
	defer entrada.Close()
	// se conecta al push del gt en el puerto 5557
	if err := entrada.Connect(*gt); err != nil {
		log.Fatalf("no se pudo conectar al gestor: %v", err)
	}
	// aqui se crea el cliente req hacia persistencia, el mismo que usa rmc
	per, err := communication.NuevoClienteReq(*persistencia, *timeout)
	if err != nil {
		log.Fatal(err)
	}
	// cierra el cliente al terminar
	defer per.Cerrar()
	// TOLERANCIA A FALLAS, un reintento con persistencia con el mismo id, la cancelacion es idempotente
	per.Reintentos = 1
	// avisa que el servicio ya esta listo
	log.Printf("listo, recibe del gestor en %s y habla con persistencia en %s", *gt, *persistencia)

	// ciclo infinito, atiende una cancelacion a la vez
	for {
		// espera una cancelacion del gt
		mensaje, err := entrada.RecvBytes(0)
		if err != nil {
			log.Printf("no se pudo recibir: %v", err)
			continue
		}
		// aqui se crea la solicitud a partir del json
		var s model.Solicitud
		if err := json.Unmarshal(mensaje, &s); err != nil {
			log.Printf("llego una cancelacion que no se pudo leer: %v", err)
			continue
		}
		// paso 1, le pide a persistencia liberar los asientos
		var resp model.Respuesta
		if err := per.Pedir(model.PeticionDB{Operacion: model.OpCancelarReserva, Solicitud: s}, &resp); err != nil {
			// persistencia no contesto, se le avisa al cliente que no se pudo
			log.Printf("%s, %s: persistencia no contesto (%v)", s.Resumen(), model.Resultado(false), err)
			resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "persistencia no contesto, la reserva no se cancelo, intente otra vez"}
		} else {
			// deja registro del resultado
			log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(resp.Exito), resp.Mensaje)
		}
		// marca quien atendio la cancelacion
		resp.Instancia = "cancelaciones"
		// paso 2, le avisa al cliente el resultado en la direccion que dejo
		if err := notificar(s.DireccionNotificacion, resp, *espera); err != nil {
			log.Printf("no se pudo notificar a %s en %s: %v", s.IdCliente, s.DireccionNotificacion, err)
			continue
		}
		log.Printf("notificacion enviada a %s en %s", s.IdCliente, s.DireccionNotificacion)
	}
}

// notificar abre un push hacia la direccion del cliente, manda la respuesta y cierra.
// el cliente es el que hace bind con un pull, asi que aqui se hace connect
func notificar(direccion string, resp model.Respuesta, espera time.Duration) error {
	// aqui se crea el socket push hacia el cliente
	socket, err := zmq.NewSocket(zmq.PUSH)
	if err != nil {
		return err
	}
	// al cerrar sigue intentando entregar el mensaje hasta este tiempo y luego lo descarta,
	// asi un cliente que ya se fue no deja bloqueado al servicio
	socket.SetLinger(espera)
	// cierra el socket al terminar, aqui es donde se termina de entregar
	defer socket.Close()
	// se conecta a la direccion que dejo el cliente
	if err := socket.Connect(direccion); err != nil {
		return err
	}
	// convierte la respuesta a json
	datos, _ := json.Marshal(resp)
	// la manda, si la conexion todavia no esta lista queda en cola hasta el linger
	_, err = socket.SendBytes(datos, 0)
	return err
}