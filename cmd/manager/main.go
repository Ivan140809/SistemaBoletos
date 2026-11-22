// gestor de transacciones gt de la maquina 2, la puerta de entrada de todos los clientes.
// no hace logica de negocio, solo valida que la solicitud venga bien armada y la enruta a rmc.
// abre tres sockets con bind y los demas procesos se conectan a el.
// frontend router en el puerto 5555 para los clientes.
// backend router en el puerto 5556 que le pasa cada solicitud a un rmc libre y vivo.
// cancelaciones push en el puerto 5557, las cancelaciones se le pasan al servicio de cancelaciones
// y el gt le responde al cliente de una vez sin esperar a que se liberen los asientos.

// ESCALABILIDAD HORIZONTAL, el backend es donde escala la capa de negocio, el router reparte
// entre todos los rmc conectados sin importar la maquina y sin reiniciar el gt.

// TOLERANCIA A FALLAS, patron paranoid pirate, el gt y cada rmc
// se mandan heartbeats cada segundo. si un rmc pasa tres intervalos sin dar senales se da por caido,
// se saca de la lista y la solicitud que estaba atendiendo se le pasa a otro rmc con el mismo id,
// asi el cliente recibe su respuesta sin enterarse de la falla y la base no la ejecuta dos veces.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sistemaboletos/internal/model"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// mensajes de control entre el gt y los rmc
const (
	listo  = "READY"     // estado del rmc
	latido = "HEARTBEAT" // senal de vida de los dos sentidos
)

// pendiente es una solicitud que espera un rmc o que un rmc esta atendiendo
type pendiente struct {
	cliente     string    // identidad del cliente para saber a donde devolver la respuesta
	cuerpo      string    // json con la solicitud como llego
	idSolicitud string    // id de la solicitud, para reconocer su respuesta
	llego       time.Time // cuando llego a la cola asi no se queda esperando para siempre
}

// trabajador es lo que el gt sabe de cada rmc conectado
type trabajador struct {
	expira  time.Time  // cuando expira por no haber dado senales de vida
	ocupado *pendiente // que solicitud esta atendiendo, si tiene valor es porque esta ocupado
}

// broker guarda el estado del gt
type broker struct {
	frontend      *zmq.Socket            // router de los clientes
	backend       *zmq.Socket            // router de los rmc
	cancelaciones *zmq.Socket            // push hacia el servicio de cancelaciones
	trabajadores  map[string]*trabajador // rmc vivos por identidad
	caidos        map[string]bool        // rmc dados por caidos, sus mensajes atrasados se ignoran
	libres        []string               // rmc libres en orden de llegada
	cola          []pendiente            // solicitudes esperando un rmc libre
	intervalo     time.Duration          // periodo de espera de latidos
	vidas         int                    // cuantos intervalos sin senales se aguantan
	espera        time.Duration          // cuanto puede esperar una solicitud en cola
}

// main arranca el gt y atiende mensajes para siempre
func main() {
	dirClientes := flag.String("clientes", "tcp://*:5555", "endpoint donde se conectan los clientes")
	dirRMC := flag.String("rmc", "tcp://*:5556", "endpoint donde se conectan las instancias de RMC")
	dirCancelaciones := flag.String("cancelaciones", "tcp://*:5557", "endpoint donde se conecta el servicio de cancelaciones")
	// TOLERANCIA A FALLAS, cada cuanto hay heartbeat y cuantos se pueden perder antes de dar a un rmc por caido
	intervalo := flag.Duration("latido", time.Second, "cada cuanto se mandan heartbeats a los rmc")
	vidas := flag.Int("vidas", 3, "cuantos heartbeats seguidos se pueden perder antes de dar un rmc por caido")
	// TOLERANCIA A FALLAS, si no hay rmc vivos la solicitud espera un rato por si alguno se reconecta
	espera := flag.Duration("espera", 4*time.Second, "cuanto espera una solicitud en cola si no hay ningun rmc vivo")
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetPrefix("GT ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	b := &broker{trabajadores: map[string]*trabajador{}, caidos: map[string]bool{}, intervalo: *intervalo, vidas: *vidas, espera: *espera}
	// aqui se crea el router que atiende a los clientes
	b.frontend = abrir(zmq.ROUTER, *dirClientes)
	// ESCALABILIDAD HORIZONTAL, aqui se conectan todos los rmc que se lancen
	b.backend = abrir(zmq.ROUTER, *dirRMC)
	// aqui se crea el push para las cancelaciones
	b.cancelaciones = abrir(zmq.PUSH, *dirCancelaciones)
	defer b.cancelaciones.Close()
	log.Printf("listo, los clientes entran por %s, rmc por %s y cancelaciones por %s", *dirClientes, *dirRMC, *dirCancelaciones)

	// aqui se crea el poller que espera en varios sockets a la vez con un solo hilo
	poller := zmq.NewPoller()
	poller.Add(b.frontend, zmq.POLLIN)
	poller.Add(b.backend, zmq.POLLIN)

	// TOLERANCIA A FALLAS, cuando toca mandar el siguiente heartbeat a los rmc
	siguienteLatido := time.Now().Add(b.intervalo)
	for {
		// espera hasta que llegue algo o hasta que toque el heartbeat, lo que pase primero
		tiempoEspera := time.Until(siguienteLatido)
		// un tiempo negativo en el poller seria esperar para siempre, por eso minimo cero
		if tiempoEspera < 0 {
			tiempoEspera = 0
		}
		listos, err := poller.Poll(tiempoEspera)
		if err != nil {
			log.Printf("fallo el poller: %v", err)
			continue
		}
		for _, ev := range listos {
			switch ev.Socket {
			case b.frontend:
				b.desdeCliente()
			case b.backend:
				// TOLERANCIA A FALLAS, lee todo lo que tenga el backend antes de revisar quien se cayo,
				// si el gt estuvo pausado los heartbeats atrasados no hacen que de por caido a un rmc vivo
				for b.desdeRMC() {
				}
			}
		}
		// TOLERANCIA A FALLAS, cada intervalo manda heartbeats y revisa quien se cayo
		if !time.Now().Before(siguienteLatido) {
			b.latir()
			siguienteLatido = time.Now().Add(b.intervalo)
		}
		// reparte lo que haya en cola entre los rmc libres
		b.repartir()
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
func (b *broker) desdeCliente() {
	partes, err := b.frontend.RecvMessage(0)
	if err != nil || len(partes) < 3 {
		return
	}
	// la identidad dice a quien responder y el cuerpo es la solicitud
	identidad, cuerpo := partes[0], partes[len(partes)-1]

	// aqui se crea la solicitud a partir del json
	var s model.Solicitud
	if err := json.Unmarshal([]byte(cuerpo), &s); err != nil {
		responder(b.frontend, identidad, model.Respuesta{Mensaje: "no se pudo leer la solicitud"})
		return
	}
	// valida la solicitud y si viene mal armada no llega a rmc ni a la base
	if err := s.Validar(); err != nil {
		log.Printf("%s, %s: se rechaza porque %v", s.Resumen(), model.Resultado(false), err)
		responder(b.frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "solicitud rechazada, " + err.Error()})
		return
	}
	// cancelacion es asincrona, va por el canal push y el cliente no espera el resultado se le envia posteriormente
	if s.Tipo == model.TipoCancelacion {
		cancelar(b.frontend, b.cancelaciones, identidad, s, cuerpo)
		return
	}
	// ESCALABILIDAD HORIZONTAL, reenvia a algun rmc con la identidad para saber a quien devolver
	// TOLERANCIA A FALLAS, aunque no haya ningun rmc vivo se queda en la cola esperando por si alguno se reconecta o vuelve
	b.cola = append(b.cola, pendiente{cliente: identidad, cuerpo: cuerpo, idSolicitud: s.IdSolicitud, llego: time.Now()})

	// el gt no espera la respuesta aqui, llega despues por el backend en desdeRMC
}

// repartir le pasa las solicitudes en cola a los rmc libres, el que lleva mas tiempo libre primero
func (b *broker) repartir() {
	for len(b.cola) > 0 && len(b.libres) > 0 {
		// toma el primer rmc libre y la primera solicitud de la cola
		id := b.libres[0]
		b.libres = b.libres[1:]
		p := b.cola[0]
		b.cola = b.cola[1:]
		// TOLERANCIA A FALLAS, se anota que este rmc atiende esta solicitud por si se cae
		b.trabajadores[id].ocupado = &p
		// manda al rmc identidad del rmc, identidad del cliente, vacio y la solicitud
		b.backend.SendMessage(id, p.cliente, "", p.cuerpo)
	}
}

// desdeRMC recibe del backend un mensaje de control o una respuesta para un cliente
// devuelve false cuando ya no hay mas mensajes esperando
func (b *broker) desdeRMC() bool {
	partes, err := b.backend.RecvMessage(zmq.DONTWAIT)
	if err != nil {
		return false
	}
	if len(partes) < 2 {
		return true
	}
	// el router pone primero la identidad del rmc que mando el mensaje
	id := partes[0]
	// TOLERANCIA A FALLAS, cada conexion de rmc usa un nombre nuevo, asi que un nombre caido no vuelve.
	// lo que llegue de el son heartbeats atrasados de una conexion cerrada y no debe registrarlo otra vez
	if b.caidos[id] {
		return true
	}
	// TOLERANCIA A FALLAS, cualquier mensaje del rmc demuestra que sigue vivo
	t, conocido := b.trabajadores[id]
	if !conocido {
		// un rmc nuevo, o uno que se habia dado por caido y volvio
		t = &trabajador{}
		b.trabajadores[id] = t
		b.libres = append(b.libres, id)
		log.Printf("%s se registro, hay %d rmc vivos", id, len(b.trabajadores))
	}
	t.expira = time.Now().Add(time.Duration(b.vidas) * b.intervalo)

	// mensaje de control, READY o HEARTBEAT, no hay nada mas que hacer
	if len(partes) == 2 {
		return true
	}
	// respuesta, llega como identidad del rmc, identidad del cliente, vacio y json
	cliente, cuerpo := partes[1], partes[len(partes)-1]
	var r model.Respuesta
	json.Unmarshal([]byte(cuerpo), &r)
	// TOLERANCIA A FALLAS, si este rmc ya no tenia esa solicitud es una respuesta tardia de una que
	// se le paso a otro, se descarta para no mandarle dos respuestas al cliente
	if t.ocupado == nil || t.ocupado.idSolicitud != r.IdSolicitud {
		log.Printf("%s contesto tarde la solicitud %s, ya la habia atendido otro rmc, se descarta", id, r.IdSolicitud)
		return true
	}
	// el rmc queda libre otra vez y va al final de la fila
	t.ocupado = nil
	b.libres = append(b.libres, id)
	log.Printf("%s contesto la solicitud %s, %s: %s", r.Instancia, r.IdSolicitud, model.Resultado(r.Exito), r.Mensaje)
	// el router usa la identidad para entregarla al cliente correcto
	b.frontend.SendMessage(cliente, "", cuerpo)
	return true
}

// latir manda un heartbeat a cada rmc y saca a los que no han dado senales a tiempo
func (b *broker) latir() {
	ahora := time.Now()
	for id, t := range b.trabajadores {
		// TOLERANCIA A FALLAS, sigue vivo, se le manda el heartbeat del gt
		if ahora.Before(t.expira) {
			b.backend.SendMessage(id, latido)
			continue
		}
		// TOLERANCIA A FALLAS, paso el tiempo sin senales, se da por caido
		delete(b.trabajadores, id)
		b.caidos[id] = true
		b.libres = quitar(b.libres, id)
		log.Printf("%s no dio senales en %v, se da por caido, quedan %d rmc vivos", id, time.Duration(b.vidas)*b.intervalo, len(b.trabajadores))
		// TOLERANCIA A FALLAS, lo que estaba atendiendo vuelve al inicio de la cola con el mismo id,
		// otro rmc lo atiende y la base reconoce el id si la operacion ya se habia hecho
		if t.ocupado != nil {
			log.Printf("la solicitud %s que atendia %s se le pasa a otro rmc", t.ocupado.idSolicitud, id)
			b.cola = append([]pendiente{*t.ocupado}, b.cola...)
		}
	}
	// si no queda ningun rmc, a los que ya esperaron demasiado en cola se les avisa
	if len(b.trabajadores) == 0 {
		var siguen []pendiente
		for _, p := range b.cola {
			if ahora.Sub(p.llego) < b.espera {
				siguen = append(siguen, p)
				continue
			}
			responder(b.frontend, p.cliente, model.Respuesta{IdSolicitud: p.idSolicitud, Mensaje: "no hay ningun rmc conectado para atenderla"})
		}
		b.cola = siguen
	}
}

// quitar saca un id de la lista de rmc libres
func quitar(lista []string, id string) []string {
	for i, x := range lista {
		if x == id {
			return append(lista[:i], lista[i+1:]...)
		}
	}
	return lista
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

// responder le contesta al cliente directo desde el gt sin pasar por rmc
func responder(frontend *zmq.Socket, identidad string, r model.Respuesta) {
	datos, _ := json.Marshal(r)
	frontend.SendMessage(identidad, "", string(datos))
}
