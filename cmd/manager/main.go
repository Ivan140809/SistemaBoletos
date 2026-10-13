// GESTOR DE TRANSACCIONES (GT) - Máquina 2, capa de negocio.
//
// Es la "puerta de entrada" del sistema: todos los clientes hablan con este
// proceso y con ningún otro. No hace lógica de negocio (no consulta ni reserva
// nada); solo valida la forma de la solicitud y la enruta.
//
//	clientes (M1) --REQ/ROUTER--> [ GT ] --DEALER/REP--> RMC (M2)
//	                                 |
//	                                 +----PUSH/PULL----> cancelaciones (M2)  (aún sin uso)
//
// El GT abre TRES sockets, y los tres hacen Bind (es decir, el GT "se queda
// esperando" y los demás procesos son los que se conectan a él):
//   - frontend:      ROUTER, puerto 5555, atiende a los clientes.
//   - backend:       DEALER, puerto 5556, reparte el trabajo entre las instancias de RMC.
//   - cancelaciones: PUSH,   puerto 5557, para mandar cancelaciones (segunda entrega).

// En Go todo archivo empieza diciendo a qué paquete pertenece. "package main"
// es el paquete especial que produce un programa ejecutable: Go busca la
// función main() y desde ahí arranca. Cada carpeta de cmd/ es un programa distinto.
package main

// Bloque de imports. Cada línea es una librería que usamos.
// Los que no tienen punto en la ruta (encoding/json, flag, log, os) son de la
// librería estándar de Go. Los que sí (github.com/...) se descargan con "go mod download".
import (
	"encoding/json" // convertir structs <-> texto JSON
	"flag"          // leer opciones de la línea de comandos (-clientes, -rmc, ...)
	// "zmq" antes de la ruta es un alias: en el código escribimos zmq.ROUTER
	// en vez de zmq4.ROUTER. Es la librería de ZeroMQ (necesita libzmq instalada en el sistema).
	zmq "github.com/pebbe/zmq4"
	"log" // imprimir mensajes con hora
	"os"
	// Esta ruta es nuestro propio código: "sistemaboletos" es el nombre del
	// módulo (está en go.mod) e internal/model es la carpeta.
	"sistemaboletos/internal/model"
)

// main es lo primero que se ejecuta.
func main() {
	// flag.String(nombre, valorPorDefecto, ayuda) define una opción de línea de comandos.
	// OJO: devuelve un PUNTERO a un string (*string), no el string. Por eso más abajo
	// se escribe *dirClientes (el asterisco = "dame el valor al que apunta"). Aquí el
	// puntero es necesario porque el valor real solo se llena cuando se llama flag.Parse().
	// "tcp://*:5555" significa: escuchar por TCP en el puerto 5555 en todas las tarjetas de red.
	dirClientes := flag.String("clientes", "tcp://*:5555", "endpoint donde se conectan los clientes")
	dirRMC := flag.String("rmc", "tcp://*:5556", "endpoint donde se conectan las instancias de RMC")
	dirCancelaciones := flag.String("cancelaciones", "tcp://*:5557", "endpoint donde se conecta el servicio de cancelaciones")
	// Lee los argumentos reales de la terminal y llena los valores definidos arriba.
	flag.Parse()

	// Configuración del logger: imprime en la salida estándar (no en stderr, que es
	// lo normal), con el prefijo "GT " y la hora con microsegundos. Así cada línea
	// del log dice quién habla y cuándo (útil para la sustentación).
	log.SetOutput(os.Stdout)
	log.SetPrefix("GT ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// abrir() está definida más abajo: crea el socket de ese tipo y hace Bind.
	// ":=" declara una variable nueva y deduce su tipo (equivale a "var x = ..."),
	// solo se puede usar dentro de funciones.
	frontend := abrir(zmq.ROUTER, *dirClientes)
	backend := abrir(zmq.DEALER, *dirRMC)
	cancelaciones := abrir(zmq.PUSH, *dirCancelaciones)
	// defer = "ejecuta esto cuando la función termine, pase lo que pase". Sirve para
	// no olvidar cerrar recursos. (frontend y backend no se cierran porque main
	// nunca termina salvo que el proceso se mate.)
	defer cancelaciones.Close()
	log.Printf("listo, los clientes entran por %s, rmc por %s y cancelaciones por %s", *dirClientes, *dirRMC, *dirCancelaciones)

	// Un Poller permite esperar a la vez en VARIOS sockets dentro de un solo hilo:
	// se bloquea hasta que alguno tenga un mensaje listo para leer (POLLIN).
	// Es necesario porque el GT debe atender tanto lo que llega de los clientes
	// como lo que llega de RMC (las respuestas) sin quedarse pegado en uno solo.
	poller := zmq.NewPoller()
	poller.Add(frontend, zmq.POLLIN)
	poller.Add(backend, zmq.POLLIN)

	// "for {" sin condición es un ciclo infinito (en Go solo existe "for"; no hay while).
	// Si lo recuerdas del apunte: en Go la condición va ANTES del cuerpo, igual que
	// en otros lenguajes: for condicion { cuerpo }.
	for {
		// Poll(-1) = esperar indefinidamente. Devuelve la lista de sockets que
		// tienen algo y un error. Go no tiene excepciones: las funciones que
		// pueden fallar devuelven (resultado, err) y uno revisa "if err != nil".
		listos, err := poller.Poll(-1) // espera hasta que llegue algo de un cliente o de RMC
		if err != nil {
			log.Printf("fallo el poller: %v", err)
			continue // vuelve al inicio del ciclo
		}
		// "for _, x := range lista" recorre una lista; "_" descarta el índice.
		for _, listo := range listos {
			// switch comparando cuál socket fue el que despertó.
			switch listo.Socket {
			case frontend:
				// Llegó algo de un cliente -> reenviarlo a RMC.
				desdeCliente(frontend, backend)
			case backend:
				// Llegó la respuesta de un RMC -> devolverla al cliente.
				desdeRMC(frontend, backend)
			}
		}
	}
}

// abrir crea un socket ZeroMQ del tipo dado y lo deja escuchando en el endpoint.
// Firma en Go: func nombre(parametros) tipoDeRetorno { ... }
// Los parámetros se escriben "nombre tipo"; aquí "tipo zmq.Type" y "endpoint string".
// Devuelve *zmq.Socket (puntero a un socket).
func abrir(tipo zmq.Type, endpoint string) *zmq.Socket {
	socket, err := zmq.NewSocket(tipo)
	if err != nil {
		// log.Fatal imprime el error y termina el programa.
		log.Fatal(err)
	}
	// Bind = "yo me quedo en este puerto y los demás se conectan a mí".
	// (Connect es lo contrario: el que se conecta busca a quien hizo Bind).
	if err := socket.Bind(endpoint); err != nil {
		log.Fatalf("no se pudo abrir %s: %v", endpoint, err)
	}
	return socket
}

// desdeCliente procesa UNA solicitud que llegó de un cliente.
//
// Qué recibe el ROUTER: un mensaje de varias "partes" (frames):
//
//	partes[0] = identidad del cliente (la agrega ROUTER automáticamente, es como
//	            su "remitente"; sirve para saber a quién devolver la respuesta)
//	partes[1] = "" (frame vacío; lo agrega el socket REQ del cliente como delimitador)
//	partes[2] = el cuerpo: la solicitud en JSON
func desdeCliente(frontend, backend *zmq.Socket) {
	partes, err := frontend.RecvMessage(0)
	// Si falló o el mensaje no tiene las 3 partes esperadas, se ignora.
	if err != nil || len(partes) < 3 {
		return
	}
	// Asignación múltiple: identidad = primera parte, cuerpo = última.
	identidad, cuerpo := partes[0], partes[len(partes)-1]

	// Convertir el JSON (texto) en un struct de Go. "var s model.Solicitud" crea
	// una Solicitud vacía; Unmarshal la llena. Se pasa &s (la dirección de s) para
	// que Unmarshal pueda modificarla. "[]byte(cuerpo)" convierte el string a bytes.
	var s model.Solicitud
	if err := json.Unmarshal([]byte(cuerpo), &s); err != nil {
		// JSON ilegible: se le responde de una vez al cliente, sin molestar a RMC.
		responder(frontend, identidad, model.Respuesta{Mensaje: "no se pudo leer la solicitud"})
		return
	}
	// Validación: tipo conocido, mes con formato AAAA-MM, asientos >= 1, etc.
	// (el código está en model.Solicitud.Validar). Las solicitudes mal formadas
	// mueren aquí y nunca llegan a RMC ni a la base de datos.
	if err := s.Validar(); err != nil {
		log.Printf("%s, %s: se rechaza porque %v", s.Resumen(), model.Resultado(false), err)
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "solicitud rechazada, " + err.Error()})
		return
	}
	// Reenviar a RMC por el DEALER. Se mandan 3 partes: la identidad del cliente,
	// un frame vacío y el cuerpo. La identidad "viaja" dentro del mensaje para
	// que cuando RMC conteste, la respuesta traiga esa identidad de vuelta y el GT
	// sepa a qué cliente entregársela (el GT no guarda ninguna tabla de pendientes).
	// El socket REP del RMC recorta todo hasta el frame vacío, entrega solo el
	// cuerpo a su código y, al responder, vuelve a pegar identidad+vacío por delante.
	//
	// "Dontwait" = no bloquear: si no hay ningún RMC conectado, falla de inmediato
	// en vez de quedarse esperando para siempre.
	// "_ , err :=" ignora el primer valor devuelto (cantidad de bytes enviados).
	if _, err := backend.SendMessageDontwait(identidad, "", cuerpo); err != nil {
		responder(frontend, identidad, model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "no hay ningun rmc conectado para atenderla"})
		return
	}
	// Nota: aquí NO se espera la respuesta. El GT vuelve al ciclo principal y la
	// respuesta llegará más tarde por el backend (ver desdeRMC). Así el GT no se
	// bloquea mientras un RMC trabaja.
}

// desdeRMC procesa UNA respuesta que llegó de un RMC y la devuelve al cliente.
func desdeRMC(frontend, backend *zmq.Socket) {
	// Mensaje esperado: [identidadDelCliente, "", respuestaJSON]
	partes, err := backend.RecvMessage(0)
	if err != nil || len(partes) < 3 {
		return
	}
	// Se decodifica solo para poder escribir el log (quién respondió y con qué
	// resultado). Aquí se ignora el error del Unmarshal.
	var r model.Respuesta
	json.Unmarshal([]byte(partes[len(partes)-1]), &r)
	log.Printf("%s contesto la solicitud %s, %s: %s", r.Instancia, r.IdSolicitud, model.Resultado(r.Exito), r.Mensaje)
	// El mensaje se reenvía TAL CUAL por el ROUTER. ROUTER lee el primer frame
	// (la identidad del cliente), lo usa para saber a qué conexión entregarlo y lo
	// quita; el REQ del cliente recibe ["", respuesta] y se queda solo con la respuesta.
	frontend.SendMessage(partes)
}

// responder le contesta directamente a un cliente desde el GT (sin pasar por
// RMC). Se usa para errores de validación y cuando no hay RMC disponible.
// Hay que reconstruir el mensaje: [identidad, "", respuestaJSON].
func responder(frontend *zmq.Socket, identidad string, r model.Respuesta) {
	// Marshal convierte el struct a JSON. El "_" ignora el error (no debería fallar con estos tipos).
	datos, _ := json.Marshal(r)
	frontend.SendMessage(identidad, "", string(datos))
}
