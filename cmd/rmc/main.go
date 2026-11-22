// rmc de la maquina 2, la capa de negocio que atiende las operaciones sincronas.
// recibe solicitudes del gt, le pide a persistencia lo necesario y devuelve el resultado.
// atiende consulta, reserva y modificacion, la cancelacion no pasa por aqui porque es asincrona.

// ESCALABILIDAD HORIZONTAL, rmc hace connect al gt y no bind, asi se pueden levantar varios rmc
// con su -id en esta u otras maquinas y el gt les reparte las solicitudes a los que esten libres.
// rmc no guarda estado porque todo esta en la base, asi que agregar capacidad es lanzar otro rmc.
// ESCALABILIDAD HORIZONTAL, hacia abajo -persistencia acepta varias direcciones separadas por coma
// y el req reparte las peticiones entre varias instancias de persistencia.

// TOLERANCIA A FALLAS, patron paranoid pirate de la guia de zeromq capitulo 4. rmc usa un dealer,
// se registra en el gt con READY y los dos se mandan heartbeats cada segundo.
// si el gt deja de dar senales, rmc cierra el socket y se reconecta esperando cada vez mas.

// TOLERANCIA A FALLAS DEL GT, -gt puede traer el gt primario y el de respaldo separados por coma.
// rmc abre un dealer hacia cada uno, se registra en los dos y les manda heartbeats a los dos, asi
// cuando el respaldo toma el control ya tiene este rmc registrado y puede repartirle trabajo de una vez.
// cada respuesta vuelve por el mismo gt que mando la solicitud.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"runtime"
	"sistemaboletos/internal/communication"
	"sistemaboletos/internal/model"
	"strconv"
	"strings"
	"time"
)

// gestor es la conexion de este rmc con un gt
type gestor struct {
	direccion string // endpoint router del gt
	socket *zmq.Socket // dealer hacia el gt, nil mientras espera para reconectar
	restantes int // cuantos intervalos sin senales del gt quedan antes de reconectar
	espera time.Duration // cuanto esperar la proxima vez que haya que reconectar
	reconectar time.Time // cuando volver a conectar si el socket esta cerrado
}

// mensajes de control con el gt, van solos en un frame y deben ser iguales a los del gt
const (
	listo = "READY" // se registra en el gt y queda libre
	latido = "HEARTBEAT" // senal de vida en los dos sentidos
)

// direccion interna entre el bucle principal y la goroutine que atiende
const trabajo = "inproc://trabajo"

// main arranca el rmc y atiende solicitudes para siempre
func main() {
	gt := flag.String("gt", "tcp://localhost:5556", "endpoint ROUTER del Gestor de Transacciones, o primario y respaldo separados por coma")
	// ESCALABILIDAD HORIZONTAL, una o varias direcciones de persistencia separadas por coma
	persistencia := flag.String("persistencia", "tcp://localhost:5560", "endpoint(s) del Servicio de Persistencia, varios separados por coma")
	id := flag.String("id", "RMC-1", "nombre de esta instancia de RMC")
	timeout := flag.Duration("timeout", 5*time.Second, "cuanto esperar a persistencia antes de rendirse")
	// TOLERANCIA A FALLAS, reintentos hacia persistencia, sirven si hay varias y una se cae
	reintentos := flag.Int("reintentos", 1, "cuantas veces reintentar con persistencia si no contesta")
	// TOLERANCIA A FALLAS, deben coincidir con -latido y -vidas del gt
	intervalo := flag.Duration("latido", time.Second, "cada cuanto se manda heartbeat al gt")
	vidas := flag.Int("vidas", 3, "cuantos heartbeats seguidos del gt se pueden perder antes de reconectar")
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetPrefix(*id + " ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// TOLERANCIA A FALLAS, el trabajo va en otra goroutine para que los heartbeats sigan saliendo
	// aunque persistencia se demore, si no el gt creeria que este rmc se cayo mientras espera la base.
	// aqui se crea el pair inproc por donde hablan las dos goroutines, es memoria compartida sin red
	interno, err := zmq.NewSocket(zmq.PAIR)
	if err != nil {
		log.Fatal(err)
	}
	defer interno.Close()
	if err := interno.Bind(trabajo); err != nil {
		log.Fatal(err)
	}
	go trabajar(*persistencia, *timeout, *reintentos, *id)

	// TOLERANCIA A FALLAS, se conecta a cada gt de la lista y se registra en todos
	var gestores []*gestor
	for _, ep := range strings.Split(*gt, ",") {
		if ep = strings.TrimSpace(ep); ep != "" {
			gestores = append(gestores, &gestor{direccion: ep, socket: conectar(ep, *id), restantes: *vidas, espera: time.Second})
		}
	}
	log.Printf("listo, recibe del gestor en %s y habla con persistencia en %s", *gt, *persistencia)

	siguienteLatido := time.Now().Add(*intervalo)
	for {
		// aqui se crea el poller con los gt conectados y el socket del trabajo, se arma cada vuelta
		// porque los sockets de los gt cambian al reconectar
		poller := zmq.NewPoller()
		poller.Add(interno, zmq.POLLIN)
		for _, g := range gestores {
			if g.socket != nil {
				poller.Add(g.socket, zmq.POLLIN)
			}
		}
		// espera hasta que llegue algo o hasta que toque el heartbeat, minimo cero
		tiempoEspera := time.Until(siguienteLatido)
		if tiempoEspera < 0 {
			tiempoEspera = 0
		}
		listos, err := poller.Poll(tiempoEspera)
		if err != nil {
			log.Printf("fallo el poller: %v", err)
			continue
		}
		for _, l := range listos {
			if l.Socket == interno {
				// la respuesta ya procesada llega con el numero del gt que la pidio y vuelve por ese gt
				partes, err := interno.RecvMessage(0)
				if err != nil || len(partes) < 4 {
					continue
				}
				n, _ := strconv.Atoi(partes[0])
				if n < len(gestores) && gestores[n].socket != nil {
					gestores[n].socket.SendMessage(partes[1:])
				}
				continue
			}
			for n, g := range gestores {
				if l.Socket != g.socket {
					continue
				}
				partes, err := g.socket.RecvMessage(0)
				if err != nil {
					break
				}
				// TOLERANCIA A FALLAS, cualquier mensaje del gt demuestra que sigue vivo
				g.restantes = *vidas
				g.espera = time.Second
				// una solicitud llega como identidad del cliente, vacio y json, se pasa al trabajo con
				// el numero del gt para saber por donde devolverla. un HEARTBEAT llega solo y no necesita mas
				if len(partes) == 3 {
					interno.SendMessage(strconv.Itoa(n), partes[0], partes[1], partes[2])
				}
				break
			}
		}
		ahora := time.Now()
		// TOLERANCIA A FALLAS, vuelve a conectar los gt cuya espera ya paso
		for _, g := range gestores {
			if g.socket == nil && !ahora.Before(g.reconectar) {
				g.socket = conectar(g.direccion, *id)
				g.restantes = *vidas
			}
		}
		// TOLERANCIA A FALLAS, cada intervalo cuenta un silencio de cada gt y le manda su heartbeat.
		// se cuenta por tiempo y no por vueltas sin mensajes, asi las respuestas del trabajo no tapan un gt caido
		if !ahora.Before(siguienteLatido) {
			for _, g := range gestores {
				if g.socket == nil {
					continue
				}
				g.restantes--
				if g.restantes > 0 {
					g.socket.Send(latido, 0)
					continue
				}
				// TOLERANCIA A FALLAS, este gt no da senales, se cierra su socket y se reconecta despues.
				// la espera se duplica cada vez hasta 32 segundos para no saturar al gt cuando vuelva.
				// no se duerme aqui para seguir atendiendo al otro gt mientras tanto
				log.Printf("el gestor %s no da senales, se reconecta en %v", g.direccion, g.espera)
				g.socket.Close()
				g.socket = nil
				g.reconectar = ahora.Add(g.espera)
				if g.espera < 32*time.Second {
					g.espera *= 2
				}
			}
			siguienteLatido = ahora.Add(*intervalo)
		}
	}
}

// conectar crea el socket dealer hacia el gt y manda READY para registrarse
func conectar(gt, id string) *zmq.Socket {
	// aqui se crea el socket dealer
	socket, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		log.Fatal(err)
	}
	// TOLERANCIA A FALLAS, nombre nuevo en cada conexion. con un nombre fijo un rmc que se reinicia rapido
	// llegaria con el nombre del caido, el gt lo seguiria viendo ocupado y la solicitud que tenia se perderia
	aleatorio := make([]byte, 3)
	rand.Read(aleatorio)
	socket.SetIdentity(id + "-" + hex.EncodeToString(aleatorio))
	// al cerrar no se queda esperando a mandar lo pendiente
	socket.SetLinger(0)
	// ESCALABILIDAD HORIZONTAL, connect y no bind para poder sumar mas rmc sin tocar el gt
	if err := socket.Connect(gt); err != nil {
		log.Fatalf("no se pudo conectar al GT: %v", err)
	}
	// se registra y queda libre para recibir solicitudes
	socket.Send(listo, 0)
	return socket
}

// trabajar recibe solicitudes por el pair interno, las atiende con persistencia y devuelve la respuesta
func trabajar(persistencia string, timeout time.Duration, reintentos int, id string) {
	// los sockets de esta goroutine siempre se usan desde el mismo hilo
	runtime.LockOSThread()
	// aqui se crea la otra punta del pair interno
	interno, err := zmq.NewSocket(zmq.PAIR)
	if err != nil {
		log.Fatal(err)
	}
	defer interno.Close()
	if err := interno.Connect(trabajo); err != nil {
		log.Fatal(err)
	}
	// aqui se crea el cliente req hacia persistencia, solo lo usa esta goroutine
	per, err := communication.NuevoClienteReq(persistencia, timeout)
	if err != nil {
		log.Fatal(err)
	}
	defer per.Cerrar()
	// TOLERANCIA A FALLAS, si persistencia no contesta se reintenta con el mismo id de solicitud
	per.Reintentos = reintentos
	per.AlReintentar = func(intento int, err error) {
		log.Printf("persistencia no contesto (%v), reintento %d de %d", err, intento, reintentos)
	}
	for {
		// llega el numero del gt, identidad del cliente, vacio y la solicitud
		partes, err := interno.RecvMessage(0)
		if err != nil || len(partes) < 4 {
			continue
		}
		// el numero del gt va y vuelve sin tocarse, el resto es la solicitud de siempre
		gt, partes := partes[0], partes[1:]
		var s model.Solicitud
		var resp model.Respuesta
		if err := json.Unmarshal([]byte(partes[2]), &s); err != nil {
			resp = model.Respuesta{Mensaje: "no se pudo leer la solicitud"}
		} else {
			resp = procesar(per, s)
		}
		// TOLERANCIA A FALLAS, el gt reconoce la respuesta por este id y descarta las tardias
		resp.IdSolicitud = s.IdSolicitud
		resp.Instancia = id
		datos, _ := json.Marshal(resp)
		// devuelve el numero del gt, identidad del cliente, vacio y la respuesta
		interno.SendMessage(gt, partes[0], partes[1], string(datos))
	}
}

// procesar mira el tipo de solicitud y la manda a consultar, reservar o modificar
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
	// TOLERANCIA A FALLAS, si llega con reserva es que otro rmc ya la hizo antes de caerse,
	// persistencia devolvio la respuesta guardada y no hay que rechazarla por falta de asientos
	if disponibilidad.Reserva != nil {
		log.Printf("%s, %s: ya estaba hecha, %s", s.Resumen(), model.Resultado(disponibilidad.Exito), disponibilidad.Mensaje)
		return disponibilidad
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
