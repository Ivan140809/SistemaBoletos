// servicio de persistencia de la maquina 3, la capa de almacenamiento del sistema.
// es el unico proceso que habla con la base de datos en supabase, recibe las peticiones
// de rmc por el puerto 5560, las ejecuta en la base y devuelve el resultado.
//
// para escalar usa el patron multithreaded server, un router hacia afuera, un dealer
// interno y varios workers en paralelo, cada uno con su propia conexion a la base.
// al arrancar puede reiniciar la base con -inicial o -generar, con -ver solo muestra
// el estado y sale, y despues de cada reserva exitosa vuelve a imprimir la tabla.
//
// ESCALABILIDAD VERTICAL, los workers aprovechan mejor una sola maquina y los rmc no cambian.
// ESCALABILIDAD HORIZONTAL, se pueden correr varias copias en m3, m4 y demas contra la misma base,
// solo la primera se arranca con -inicial o -generar para no borrar los datos de las otras.
// escalabilidad de lectura: con -dsn-lectura las consultas de eventos van a una replica.
// la funcion sql srb.crear_reserva evita la sobreventa aunque haya muchos workers.
//

package main

import (
	"context"       // pone limite de tiempo a las operaciones
	"encoding/json" // convierte mensajes a json y de vuelta
	"flag"          // lee las opciones de la terminal
	"fmt"           // imprime texto en pantalla
	"log"           // imprime mensajes con la hora
	"os"            // lee variables de entorno
	"runtime"       // permite fijar una goroutine a un hilo
	"time"          // maneja duraciones de tiempo

	zmq "github.com/pebbe/zmq4" // libreria de zeromq para los sockets

	"sistemaboletos/internal/db"    // acceso a la base de datos
	"sistemaboletos/internal/model" // tipos compartidos del sistema
)

// main arranca todo el servicio
func main() {
	// puerto donde escucha el servicio
	direccion := flag.String("bind", "tcp://*:5560", "endpoint REP donde escucha el servicio")
	// cadena de conexion a supabase, sale del .env que carga machine3_storage.sh
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "cadena de conexion a supabase, por defecto la de DATABASE_URL")
	// archivo json para cargar la base desde cero
	inicial := flag.String("inicial", "", "archivo json con el estado inicial")
	// si es true se inventan eventos de prueba
	generar := flag.Bool("generar", false, "genera eventos de prueba automaticamente")
	// mes de los eventos inventados
	mes := flag.String("mes", "2026-11", "mes de los eventos generados")
	// cuantos eventos se inventan
	eventos := flag.Int("eventos", 50, "cantidad de eventos a generar")
	// cuantos eventos se repiten dos veces
	dobles := flag.Int("dobles", 20, "cuantos de esos eventos se repiten dos veces en el mes")
	// asientos de cada fecha
	asientos := flag.Int("asientos", 100, "asientos por ocurrencia")
	// si es true se imprime la tabla despues de cada reserva
	tabla := flag.Bool("tabla", true, "muestra la base despues de cada reserva")
	// si es true solo se muestra la base y se sale
	ver := flag.Bool("ver", false, "muestra como esta la base y se sale")
	// tiempo maximo de cada operacion en la base
	limite := flag.Duration("limite", 5*time.Second, "tiempo maximo para cada operacion en la base")
	// ESCALABILIDAD VERTICAL, cuantos workers atienden en paralelo
	workers := flag.Int("workers", 4, "cuantas peticiones atiende persistencia en paralelo")
	// escalabilidad de lectura, cadena de la replica opcional
	dsnLectura := flag.String("dsn-lectura", os.Getenv("DATABASE_URL_LECTURA"), "cadena de conexion a una replica de solo lectura (opcional)")
	// lee todas las opciones que se pasaron
	flag.Parse()
	// nunca menos de un worker
	if *workers < 1 {
		*workers = 1
	}

	// los mensajes salen por la terminal
	log.SetOutput(os.Stdout)
	// cada mensaje empieza con persistencia
	log.SetPrefix("persistencia ")
	// cada mensaje lleva la hora con microsegundos
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// sin cadena de conexion no se puede seguir
	if *dsn == "" {
		log.Fatal("no hay a donde conectarse, falta DATABASE_URL en el .env o el flag -dsn")
	}
	// ESCALABILIDAD VERTICAL, una conexion por worker y una extra para imprimir la tabla
	conexiones := int32(*workers + 1)
	// aqui se crea el pool de conexiones a supabase y se aplica schema.sql
	base, err := db.Conectar(*dsn, conexiones)
	// si no conecta se detiene el programa
	if err != nil {
		log.Fatal(err)
	}
	// cierra la base al terminar
	defer base.Cerrar()
	// si hay replica se usa para las consultas de eventos
	if *dsnLectura != "" {
		// aca se crea la conexion a la replica de lectura
		if err := base.UsarReplica(*dsnLectura, conexiones); err != nil {
			log.Fatal(err)
		}
		log.Printf("las consultas de eventos van a la replica de lectura")
	}

	// con -ver se imprime la base y se sale sin borrar nada
	if *ver {
		mostrarTabla(base, *limite)
		return
	}
	// si hay estado inicial se borra la base y se carga de nuevo
	if *generar || *inicial != "" {
		// lista de eventos que se van a cargar
		var iniciales []model.Evento
		// aca se crean los eventos, inventados o leidos del json
		if *generar {
			iniciales, err = db.GenerarEventos(*mes, *eventos, *dobles, *asientos)
		} else {
			iniciales, err = db.LeerEventos(*inicial)
		}
		// si se armaron bien se borra la base y se cargan
		if err == nil {
			err = base.Reiniciar(context.Background(), iniciales)
		}
		// si algo fallo se detiene el programa
		if err != nil {
			log.Fatalf("no se pudo cargar el estado inicial: %v", err)
		}
		log.Printf("base reiniciada con %d eventos", len(iniciales))
	}

	// ESCALABILIDAD VERTICAL, aca se crea el router, el socket que ven los rmc desde afuera
	frontend, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		log.Fatal(err)
	}
	// cierra el router al terminar
	defer frontend.Close()
	// abre el puerto 5560 para que los rmc se conecten
	if err := frontend.Bind(*direccion); err != nil {
		log.Fatalf("no se pudo abrir %s: %v", *direccion, err)
	}
	// ESCALABILIDAD VERTICAL, aca se crea el dealer interno que reparte peticiones entre los workers
	backend, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		log.Fatal(err)
	}
	// cierra el dealer al terminar
	defer backend.Close()
	// abre la direccion interna antes de que los workers se conecten
	if err := backend.Bind("inproc://workers"); err != nil {
		log.Fatal(err)
	}

	// aca se crea el canal de avisos para reimprimir la tabla, con espacio para uno
	avisos := make(chan struct{}, 1)
	// si la tabla esta activa se lanza la goroutine que la imprime
	if *tabla {
		go imprimirTabla(base, *limite, avisos)
	}
	// ESCALABILIDAD VERTICAL, aca se crean los workers, cada uno en su propia goroutine
	for i := 1; i <= *workers; i++ {
		go trabajador(i, base, *limite, avisos) // cada worker se conecta al dealer interno
	}
	// avisa que el servicio ya esta listo
	log.Printf("listo, escuchando en %s con %d workers", *direccion, *workers)
	// muestra como arranca la base
	fmt.Println("estado inicial de la base")
	mostrarTabla(base, *limite)

	// ESCALABILIDAD VERTICAL, el proxy pasa mensajes entre router y dealer para siempre
	if err := zmq.Proxy(frontend, backend, nil); err != nil {
		log.Fatalf("fallo el proxy: %v", err)
	}
}

// trabajador recibe peticiones del dealer, las hace en la base y responde
func trabajador(n int, base *db.BaseDatos, limite time.Duration, avisos chan struct{}) {
	// el socket siempre se usa desde el mismo hilo
	runtime.LockOSThread()
	// ESCALABILIDAD VERTICAL, aca se crea el socket rep propio de este worker
	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	// cierra el socket al terminar
	defer socket.Close()
	// se conecta al dealer interno
	if err := socket.Connect("inproc://workers"); err != nil {
		log.Fatal(err)
	}
	// repite para siempre
	for {
		// espera una peticion
		mensaje, err := socket.RecvBytes(0)
		// si falla se salta y espera la siguiente
		if err != nil {
			log.Printf("worker %d no pudo recibir: %v", n, err)
			continue
		}
		// le pone un tiempo maximo a la operacion
		ctx, cancelar := context.WithTimeout(context.Background(), limite)
		// hace la operacion en la base
		respuesta, cambio := atender(ctx, base, mensaje)
		// libera el tiempo maximo
		cancelar()
		// convierte la respuesta a json
		datos, _ := json.Marshal(respuesta)
		// un socket rep siempre tiene que responder
		if _, err := socket.SendBytes(datos, 0); err != nil {
			log.Printf("worker %d no pudo responder: %v", n, err)
		}
		// si hubo una reserva se pide reimprimir la tabla
		if cambio {
			// avisa sin bloquear y si ya hay un aviso pendiente se descarta
			select {
			case avisos <- struct{}{}:
			default:
			}
		}
	}
}

// imprimirTabla reimprime la base cada vez que llega un aviso
func imprimirTabla(base *db.BaseDatos, limite time.Duration, avisos chan struct{}) {
	// espera avisos para siempre
	for range avisos {
		mostrarTabla(base, limite)
	}
}

// atender hace la operacion pedida y dice si se creo una reserva
func atender(ctx context.Context, base *db.BaseDatos, mensaje []byte) (model.Respuesta, bool) {
	// aqui se crea la peticion a partir del json que llego
	var peticion model.PeticionDB
	if err := json.Unmarshal(mensaje, &peticion); err != nil {
		return model.Respuesta{Mensaje: "no se pudo leer la peticion: " + err.Error()}, false
	}
	// datos de la solicitud del cliente
	s := peticion.Solicitud
	// respuesta que se va a devolver
	var resp model.Respuesta
	// error si algo sale mal
	var err error
	// segun la operacion se llama al metodo de db que toca
	switch peticion.Operacion {
	// consultar los eventos de un mes
	case model.OpConsultarEventos:
		var eventos []model.Evento
		eventos, err = base.ConsultarEventos(ctx, s.Mes)
		// mensaje con cuantos eventos se encontraron
		encontrados := fmt.Sprintf("se encontraron %d eventos en %s", len(eventos), s.Mes)
		if len(eventos) == 1 {
			encontrados = fmt.Sprintf("se encontro 1 evento en %s", s.Mes)
		}
		// aqui se crea la respuesta con los eventos
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Exito: true, Mensaje: encontrados, Eventos: eventos}
	// consultar los asientos libres de una fecha
	case model.OpConsultarDisponibilidad:
		var disponibles int
		disponibles, err = base.Disponibilidad(ctx, s.IdEvento, s.IdOcurrencia)
		// aqui se crea la respuesta con los asientos libres
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Exito: true, Mensaje: fmt.Sprintf("%s en la fecha %d tiene %d asientos libres", s.IdEvento, s.IdOcurrencia, disponibles), AsientosDisponibles: disponibles}
	// aqui se crea la reserva en la base
	case model.OpCrearReserva:
		resp, err = base.CrearReserva(ctx, s)
	// operacion que no existe
	default:
		err = fmt.Errorf("no se reconoce la operacion %q", peticion.Operacion)
	}
	// si algo fallo se responde solo con el error
	if err != nil {
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: err.Error()}
	}
	// deja registro de lo que paso
	log.Printf("%s (%s), %s: %s", s.Resumen(), queHace(peticion.Operacion), model.Resultado(resp.Exito), resp.Mensaje)
	// true solo si se creo una reserva con exito
	cambio := resp.Exito && peticion.Operacion == model.OpCrearReserva
	return resp, cambio
}

// mostrarTabla imprime los eventos con sus asientos y la lista de reservas
func mostrarTabla(base *db.BaseDatos, limite time.Duration) {
	// le pone un tiempo maximo a la lectura
	ctx, cancelar := context.WithTimeout(context.Background(), limite)
	// libera el tiempo maximo al terminar
	defer cancelar()
	// arma el texto de la tabla desde la base
	tabla, err := base.Tabla(ctx, true)
	// si no se pudo leer se avisa y se sale
	if err != nil {
		log.Printf("no se pudo leer la base: %v", err)
		return
	}
	// imprime la tabla
	fmt.Print(tabla)
}

// queHace traduce la operacion a una frase corta para el log
func queHace(op model.OperacionDB) string {
	switch op {
	case model.OpConsultarEventos:
		return "busca eventos"
	case model.OpConsultarDisponibilidad:
		return "revisa cupos"
	case model.OpCrearReserva:
		return "guarda la reserva"
	}
	// si no es ninguna se deja el nombre tal cual
	return string(op)
}
