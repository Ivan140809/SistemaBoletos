// gestor de transacciones (gt), maquina 2 (capa de negocio).
//
// es la puerta de entrada del sistema: todos los clientes hablan con este
// proceso y con ningun otro. no hace logica de negocio (no consulta ni reserva
// nada), solo valida que la solicitud venga bien armada y la enruta.
//
//	clientes (m1) --req/router--> [ gt ] --dealer/rep--> rmc (m2)
//	                                 |
//	                                 +----push/pull----> cancelaciones (m2)  (aun sin uso)
//
// el gt abre tres sockets y los tres hacen bind, o sea que el gt se queda
// esperando y los demas procesos son los que se conectan a el:
//   - frontend:      router, puerto 5555, atiende a los clientes.
//   - backend:       dealer, puerto 5556, reparte el trabajo entre las instancias de rmc.
//   - cancelaciones: push,   puerto 5557, para mandar cancelaciones (segunda entrega).

// todo archivo de go empieza diciendo a que paquete pertenece. "package main" es
// el paquete especial que genera un ejecutable: go busca main() y arranca por
// ahi. cada carpeta de cmd/ es un programa distinto.
package main

// los imports son las librerias que usamos. los que no tienen punto en la ruta
// (encoding/json, flag, log, os) son de la libreria estandar. los que si
// (github.com/...) se bajan con "go mod download".
import (
	"encoding/json" // pasar structs <-> texto json
	"flag"          // leer opciones de la terminal (-clientes, -rmc, ...)
	// "zmq" antes de la ruta es un alias, asi escribimos zmq.router en vez de
	// zmq4.router. es la libreria de zeromq y necesita libzmq instalada en el sistema.
	zmq "github.com/pebbe/zmq4"
	"log" // imprimir mensajes con hora
	"os"
	// esta ruta es codigo nuestro: "sistemaboletos" es el nombre del modulo
	// (esta en go.mod) e internal/model es la carpeta.
	"sistemaboletos/internal/model"
)

// main es lo primero que corre.
func main() {
	// flag.string(nombre, valorpordefecto, ayuda) define una opcion de la terminal.
	// ojo que devuelve un puntero (*string), no el string, por eso mas abajo se
	// escribe *dirclientes (el asterisco es "dame el valor al que apunta"). tiene
	// que ser puntero porque el valor real solo se llena cuando se llama flag.parse().
	// "tcp://*:5555" es escuchar por tcp en el puerto 5555 en todas las tarjetas de red.
	dirClientes := flag.String("clientes", "tcp://*:5555", "endpoint donde se conectan los clientes")
	dirRMC := flag.String("rmc", "tcp://*:5556", "endpoint donde se conectan las instancias de RMC")
	dirCancelaciones := flag.String("cancelaciones", "tcp://*:5557", "endpoint donde se conecta el servicio de cancelaciones")
	// lee lo que se escribio en la terminal y llena las opciones de arriba.
	flag.Parse()

	// el logger imprime en la salida estandar (no en stderr, que es lo normal),
	// con el prefijo "gt " y la hora con microsegundos. asi cada linea dice quien
	// habla y cuando, lo cual sirve para la sustentacion.
	log.SetOutput(os.Stdout)
	log.SetPrefix("GT ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// abrir() esta mas abajo: crea el socket de ese tipo y le hace bind.
	// ":=" declara una variable nueva y deduce el tipo, solo se puede usar dentro
	// de funciones.
	frontend := abrir(zmq.ROUTER, *dirClientes)
	backend := abrir(zmq.DEALER, *dirRMC)
	cancelaciones := abrir(zmq.PUSH, *dirCancelaciones)
	// defer corre esa linea cuando la funcion termina, pase lo que pase, y asi no
	// se nos olvida cerrar cosas. frontend y backend no se cierran porque main
	// nunca termina a menos que maten el proceso.
	defer cancelaciones.Close()
	log.Printf("listo, los clientes entran por %s, rmc por %s y cancelaciones por %s", *dirClientes, *dirRMC, *dirCancelaciones)

	// un poller deja esperar en varios sockets a la vez con un solo hilo: se
	// bloquea hasta que alguno tenga algo para leer (pollin). lo necesitamos
	// porque el gt tiene que atender lo que llega de los clientes y lo que llega
	// de rmc (las respuestas) sin quedarse pegado en uno solo.
	poller := zmq.NewPoller()
	poller.Add(frontend, zmq.POLLIN)
	poller.Add(backend, zmq.POLLIN)

	// "for {" sin condicion es un ciclo infinito (en go solo existe for, no hay while).
	for {
		// poll(-1) espera sin limite. devuelve los sockets que tienen algo y un
		// error. go no tiene excepciones: lo que puede fallar devuelve
		// (resultado, err) y uno revisa "if err != nil".
		listos, err := poller.Poll(-1) // espera hasta que llegue algo de un cliente o de rmc
		if err != nil {
			log.Printf("fallo el poller: %v", err)
			continue // vuelve al inicio del ciclo
		}
		// "for _, x := range lista" recorre la lista, el "_" descarta el indice.
		for _, listo := range listos {
			// se mira cual socket fue el que desperto.
			switch listo.Socket {
			case frontend:
				// llego algo de un cliente, se reenvia a rmc.
				desdeCliente(frontend, backend)
			case backend:
				// llego la respuesta de un rmc, se devuelve al cliente.
				desdeRMC(frontend, backend)
			}
		}
	}
}

// abrir crea un socket zeromq del tipo que le pidan y lo deja escuchando en el
// endpoint. en go la firma es func nombre(parametros) tipoderetorno, y los
// parametros se escriben "nombre tipo". devuelve *zmq.socket (un puntero al socket).
func abrir(tipo zmq.Type, endpoint string) *zmq.Socket {
	socket, err := zmq.NewSocket(tipo)
	if err != nil {
		// log.fatal imprime el error y mata el programa.
		log.Fatal(err)
	}
	// bind es "yo me quedo en este puerto y los demas se conectan a mi". connect
	// es lo contrario: el que se conecta busca a quien hizo bind.
	if err := socket.Bind(endpoint); err != nil {
		log.Fatalf("no se pudo abrir %s: %v", endpoint, err)
	}
	return socket
}

// desdecliente procesa una solicitud que llego de un cliente.
//
// lo que recibe el router es un mensaje en varias partes (frames):
//
//	partes[0] = identidad del cliente (la pone el router solo, es como el
//	            remitente, sirve para saber a quien devolver la respuesta)
//	partes[1] = "" (frame vacio, lo agrega el req del cliente como separador)
//	partes[2] = el cuerpo: la solicitud en json
func desdeCliente(frontend, backend *zmq.Socket) {
	partes, err := frontend.RecvMessage(0)
	// si fallo o no trae las 3 partes esperadas, se ignora.
	if err != nil || len(partes) < 3 {
		return
	}
	// asignacion multiple: identidad es la primera parte y cuerpo la ultima.
	identidad, cuerpo := partes[0], partes[len(partes)-1]

	// pasa el json (texto) a un struct de go. "var s model.solicitud" crea una
	// solicitud vacia y unmarshal la llena. se pasa &s (su direccion) para que
	// unmarshal pueda modificarla. "[]byte(cuerpo)" convierte el string a bytes.
	var s model.Solicitud
	if err := json.Unmarshal([]byte(cuerpo), &s); err != nil {
		// json ilegible: se le responde al cliente de una vez, sin molestar a rmc.
		responder(frontend, identidad, model.Respuesta{Mensaje: "no se pudo leer la solicitud"})
		return
	}
	// validacion: tipo conocido, mes con formato aaaa-mm, asientos >= 1, etc.
	// (el codigo esta en model.solicitud.validar). lo que venga mal armado muere
	// aqui y nunca llega a rmc ni a la base.
	if err := s.Validar(); err != nil {
		log.Printf("%s, %s: se rechaza porque %v", s.Resumen(), model.Resultado(false), err)
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "solicitud rechazada, " + err.Error()})
		return
	}
	// se reenvia a rmc por el dealer, en 3 partes: identidad del cliente, frame
	// vacio y cuerpo. la identidad viaja dentro del mensaje para que cuando rmc
	// conteste, la respuesta la traiga de vuelta y el gt sepa a que cliente
	// entregarla (asi el gt no tiene que guardar ninguna tabla de pendientes).
	// el rep del rmc recorta todo hasta el frame vacio, le pasa solo el cuerpo a su
	// codigo y al responder vuelve a pegar identidad + vacio por delante.
	//
	// "dontwait" es no bloquear: si no hay ningun rmc conectado falla de una vez
	// en vez de quedarse esperando para siempre.
	// "_ , err :=" ignora el primer valor que devuelve (los bytes enviados).
	if _, err := backend.SendMessageDontwait(identidad, "", cuerpo); err != nil {
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "no hay ningun rmc conectado para atenderla"})
		return
	}
	// ojo: aqui no se espera la respuesta. el gt vuelve al ciclo principal y la
	// respuesta llega despues por el backend (ver desdermc). asi no se bloquea
	// mientras un rmc trabaja.
}

// desdermc procesa una respuesta que llego de un rmc y se la devuelve al cliente.
func desdeRMC(frontend, backend *zmq.Socket) {
	// el mensaje esperado es [identidaddelcliente, "", respuestajson]
	partes, err := backend.RecvMessage(0)
	if err != nil || len(partes) < 3 {
		return
	}
	// se decodifica solo para poder escribir el log (quien respondio y con que
	// resultado). aqui se ignora el error del unmarshal.
	var r model.Respuesta
	json.Unmarshal([]byte(partes[len(partes)-1]), &r)
	log.Printf("%s contesto la solicitud %s, %s: %s", r.Instancia, r.IdSolicitud, model.Resultado(r.Exito), r.Mensaje)
	// el mensaje se reenvia tal cual por el router. el router lee el primer frame
	// (la identidad del cliente), lo usa para saber a que conexion entregarlo y lo
	// quita, asi que el req del cliente recibe ["", respuesta] y se queda solo con
	// la respuesta.
	frontend.SendMessage(partes)
}

// responder le contesta a un cliente directo desde el gt, sin pasar por rmc. se
// usa para errores de validacion y cuando no hay rmc disponible.
// hay que armar el mensaje a mano: [identidad, "", respuestajson].
func responder(frontend *zmq.Socket, identidad string, r model.Respuesta) {
	// marshal pasa el struct a json. el "_" ignora el error, no deberia fallar con estos tipos.
	datos, _ := json.Marshal(r)
	frontend.SendMessage(identidad, "", string(datos))
}
