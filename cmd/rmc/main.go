// RMC (Reservas, Modificaciones y Consultas) - Máquina 2, capa de negocio.
//
// Es quien realmente "piensa" las operaciones síncronas. Recibe solicitudes del
// Gestor de Transacciones (GT), decide qué hay que preguntarle o pedirle a la
// capa de persistencia y devuelve el resultado.
//
//	GT --(DEALER/REP)--> [ RMC ] --(REQ/REP)--> persistencia (M3)
//
// Estado actual: solo implementa CONSULTA y RESERVA. La MODIFICACIÓN de reservas
// no está implementada (no es parte de la primera entrega).
//
// Detalle de diseño importante: RMC hace Connect al GT (no Bind). Como el GT es
// el que "se queda esperando", se pueden arrancar varias instancias de RMC
// (con distinto -id) apuntando al mismo GT y el DEALER del GT les reparte las
// solicitudes por turnos (round-robin). Esa es la base de la escalabilidad de la
// segunda entrega: no habría que tocar el GT.
package main

import (
	"encoding/json"
	"flag"
	"fmt" // formatear texto: fmt.Sprintf arma un string con %s, %d, etc.
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"sistemaboletos/internal/communication" // cliente REQ con timeout (para hablar con persistencia)
	"sistemaboletos/internal/model"
	"time"
)

func main() {
	// Opciones de línea de comandos (recuerda: devuelven punteros, por eso luego se usa *gt, *id, ...).
	gt := flag.String("gt", "tcp://localhost:5556", "endpoint DEALER del Gestor de Transacciones")
	persistencia := flag.String("persistencia", "tcp://localhost:5560", "endpoint del Servicio de Persistencia")
	// Nombre de esta instancia. Se incluye en cada respuesta (campo Instancia)
	// para poder ver QUÉ RMC atendió cada solicitud cuando haya varios.
	id := flag.String("id", "RMC-1", "nombre de esta instancia de RMC")
	// flag.Duration permite escribir cosas como 5s, 300ms, 2m.
	// "5*time.Second" es 5 segundos como valor por defecto.
	timeout := flag.Duration("timeout", 5*time.Second, "cuanto esperar a persistencia antes de rendirse")
	flag.Parse()

	// Logger: el prefijo es el nombre de la instancia (RMC-1, RMC-2, ...).
	log.SetOutput(os.Stdout)
	log.SetPrefix(*id + " ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// Socket REP (reply): el lado "servidor" del patrón request/reply.
	// Regla de oro de REP: estrictamente recibir -> responder -> recibir -> responder.
	// No puede recibir dos mensajes seguidos sin haber contestado el primero.
	// Por eso en el ciclo de abajo SIEMPRE se envía una respuesta, incluso si hay error.
	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	defer socket.Close()
	// Connect (no Bind): RMC va a buscar al GT. Si el GT aún no está arriba no
	// pasa nada, ZeroMQ reintenta la conexión solo en segundo plano.
	if err := socket.Connect(*gt); err != nil { // rmc se conecta al gestor, asi se pueden sumar mas instancias sin tocar nada
		log.Fatalf("no se pudo conectar al GT: %v", err)
	}
	// "per" = el canal hacia persistencia (un socket REQ envuelto en nuestro
	// ayudante ClienteReq, ver internal/communication/req.go).
	per, err := communication.NuevoClienteReq(*persistencia, *timeout)
	if err != nil {
		log.Fatal(err)
	}
	defer per.Cerrar()
	log.Printf("listo, recibe del gestor en %s y habla con persistencia en %s", *gt, *persistencia)

	// Ciclo principal: atiende solicitudes de a una (RMC es de un solo hilo).
	for {
		// Se bloquea hasta que llegue un mensaje. RecvBytes devuelve los bytes
		// ([]byte) del cuerpo; el REP ya le quitó la identidad y el frame vacío.
		mensaje, err := socket.RecvBytes(0)
		if err != nil {
			log.Printf("no se pudo recibir: %v", err)
			continue
		}
		var s model.Solicitud
		var resp model.Respuesta
		// De JSON a struct. Si el JSON está dañado se arma una respuesta de error;
		// si está bien se procesa la solicitud.
		if err := json.Unmarshal(mensaje, &s); err != nil {
			resp = model.Respuesta{Mensaje: "no se pudo leer la solicitud"}
		} else {
			resp = procesar(per, s)
		}
		// Se marca la respuesta con el nombre de esta instancia.
		resp.Instancia = *id
		datos, _ := json.Marshal(resp)
		// Responder es OBLIGATORIO en un REP (si no, el socket queda bloqueado).
		if _, err := socket.SendBytes(datos, 0); err != nil {
			log.Printf("no se pudo responder al gestor: %v", err)
		}
	}
}

// procesar decide qué hacer según el tipo de solicitud (consulta o reserva).
// "per" es un puntero (*) al canal hacia persistencia; "s" es la solicitud.
func procesar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	// switch sobre un valor: en Go cada case termina solo (no hace falta "break").
	switch s.Tipo {
	case model.TipoConsulta:
		return consultar(per, s)
	case model.TipoReserva:
		return reservar(per, s)
	}
	// Si no entró a ningún case, el tipo no se conoce (el GT ya lo habría
	// filtrado en Validar, esto es una segunda red de seguridad).
	log.Printf("%s, %s: operacion no reconocida", s.Resumen(), model.Resultado(false))
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: fmt.Sprintf("no se reconoce la operacion %s", s.Tipo)}
}

// consultar: RMC no calcula nada, solo reenvía la consulta a persistencia
// y devuelve lo que ella conteste (lista de eventos del mes con sus asientos).
// Es el flujo "1. recibir, 2. enviar a persistencia, 3. esperar y devolver" del enunciado.
func consultar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	var resp model.Respuesta
	// Pedir envía la petición y espera la respuesta, que se escribe en resp
	// (por eso se pasa &resp, la dirección de la variable).
	// PeticionDB agrupa "qué operación quiero" + "la solicitud original".
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpConsultarEventos, Solicitud: s}, &resp); err != nil {
		// Típicamente: persistencia no contestó a tiempo (timeout) o está caída.
		return sinPersistencia(s, err)
	}
	log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(resp.Exito), resp.Mensaje)
	return resp
}

// reservar sigue el flujo del enunciado en DOS viajes a persistencia:
//
//	1) preguntar cuántos asientos libres hay        (OpConsultarDisponibilidad)
//	2) si alcanzan, pedir que se guarde la reserva  (OpCrearReserva)
//
// OJO (pregunta típica de sustentación): entre el paso 1 y el 2 otro cliente
// podría haberse llevado los asientos. Por eso el paso 1 es solo una revisión
// "rápida" y la garantía real está en el paso 2: la función SQL crear_reserva
// resta los asientos con un UPDATE condicionado (asientos_disponibles >= cantidad)
// que es atómico en la base de datos. Si ya no alcanzan, la base rechaza.
func reservar(per *communication.ClienteReq, s model.Solicitud) model.Respuesta {
	// Paso 1: disponibilidad.
	var disponibilidad model.Respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpConsultarDisponibilidad, Solicitud: s}, &disponibilidad); err != nil {
		return sinPersistencia(s, err)
	}
	// Se rechaza si la consulta falló (p. ej. el evento no existe) o si piden más
	// asientos de los que quedan. Aquí no se toca la base: "queda como estaba".
	if !disponibilidad.Exito || disponibilidad.AsientosDisponibles < s.CantidadAsientos {
		resp := model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: disponibilidad.Mensaje}
		if disponibilidad.Exito {
			// La consulta salió bien pero no alcanzan: mensaje explicando cuántos quedan.
			resp.Mensaje = fmt.Sprintf("no alcanzan los asientos de %s en la fecha %d, pidio %s y %s", s.IdEvento, s.IdOcurrencia, model.Asientos(s.CantidadAsientos), model.Quedan(disponibilidad.AsientosDisponibles))
		}
		log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(false), resp.Mensaje)
		return resp
	}
	// Paso 2: confirmar la reserva en la base (a través de persistencia).
	var confirmacion model.Respuesta
	if err := per.Pedir(model.PeticionDB{Operacion: model.OpCrearReserva, Solicitud: s}, &confirmacion); err != nil {
		return sinPersistencia(s, err)
	}
	log.Printf("%s, %s: %s", s.Resumen(), model.Resultado(confirmacion.Exito), confirmacion.Mensaje)
	return confirmacion
}

// sinPersistencia arma la respuesta de error para cuando el servicio de
// persistencia no contestó (timeout o caída). El cliente verá este mensaje.
func sinPersistencia(s model.Solicitud, err error) model.Respuesta {
	log.Printf("%s, %s: persistencia no contesto (%v)", s.Resumen(), model.Resultado(false), err)
	return model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: "persistencia no contesto, intente otra vez"}
}
