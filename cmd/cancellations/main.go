package main
import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"
	zmq "github.com/pebbe/zmq4"
	"sistemaboletos/internal/communication"
	"sistemaboletos/internal/model"
)

func main() {
	gt := flag.String("gt", "tcp://localhost:5557", "endpoint PUSH del Gestor de Transacciones")
	persistencia := flag.String("persistencia", "tcp://localhost:5560", "endpoint del Servicio de Persistencia")
	timeout := flag.Duration("timeout", 5*time.Second, "tiempo máximo de espera a Persistencia")
	flag.Parse()
	log.SetOutput(os.Stdout)
	log.SetPrefix("Cancelaciones")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	entrada, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		log.Fatal(err)
	}
	defer entrada.Close()
	if err := entrada.Connect(*gt); err != nil {
		log.Fatalf("no se pudo conectar al GT %v", err)
	}
	per, err := communication.NuevoClienteReq(*persistencia, *timeout)
	if err != nil {
		log.Fatal(err)
	}
	defer per.Cerrar()
	notificadores := map[string]*zmq.Socket{} // un socket PUSH por cada cliente que espera notificaciones
	log.Printf("recibiendo cancelaciones del GT en %s Persistencia en %s", *gt, *persistencia)

	for {
		mensaje, err := entrada.RecvBytes(0)
		if err != nil {
			log.Printf("error recibiendo: %v", err)
			continue
		}
		var s model.Solicitud
		if err := json.Unmarshal(mensaje, &s); err != nil {
			log.Printf("cancelación mal formada: %v", err)
			continue
		}
		var resp model.Respuesta
		if err := per.Pedir(model.PeticionDB{Operacion: model.OpCancelarReserva, Solicitud: s}, &resp); err != nil {
			resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "el servicio de persistencia no responde"}
		}
		log.Printf("%s %s %s", s.Resumen(), model.Resultado(resp.Exito), resp.Mensaje)
		notificar(notificadores, s.DireccionNotificacion, resp)
	}
}

func notificar(notificadores map[string]*zmq.Socket, direccion string, resp model.Respuesta) {
	socket, existe := notificadores[direccion]
	if !existe {
		var err error
		if socket, err = zmq.NewSocket(zmq.PUSH); err != nil {
			log.Printf("no se pudo crear el socket de notificación: %v", err)
			return
		}
		socket.SetLinger(0)
		if err := socket.Connect(direccion); err != nil {
			log.Printf("dirección de notificación inválida %s: %v", direccion, err)
			socket.Close()
			return
		}
		notificadores[direccion] = socket
	}
	datos, _ := json.Marshal(resp)
	if _, err := socket.SendBytes(datos, zmq.DONTWAIT); err != nil { // si el cliente no escucha, el servicio no se bloquea
		log.Printf("no se pudo notificar a %s: %v", direccion, err)
		return
	}
	log.Printf("notificación enviada a %s", direccion)
}
