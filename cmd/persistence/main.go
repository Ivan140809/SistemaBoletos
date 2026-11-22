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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	zmq "github.com/pebbe/zmq4"

	"sistemaboletos/internal/db"
	"sistemaboletos/internal/model"
)

// main arranca todo el servicio
func main() {
	direccion := flag.String("bind", "tcp://*:5560", "endpoint REP donde escucha el servicio")
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "cadena de conexion a supabase, por defecto la de DATABASE_URL")
	inicial := flag.String("inicial", "", "archivo json con el estado inicial")
	generar := flag.Bool("generar", false, "genera eventos de prueba automaticamente")
	mes := flag.String("mes", "2026-11", "mes de los eventos generados")
	eventos := flag.Int("eventos", 50, "cantidad de eventos a generar")
	dobles := flag.Int("dobles", 20, "cuantos de esos eventos se repiten dos veces en el mes")
	asientos := flag.Int("asientos", 100, "asientos por ocurrencia")
	tabla := flag.Bool("tabla", true, "muestra la base despues de cada reserva")
	ver := flag.Bool("ver", false, "muestra como esta la base y se sale")
	limite := flag.Duration("limite", 5*time.Second, "tiempo maximo para cada operacion en la base")
	// ESCALABILIDAD VERTICAL, cuantos workers atienden en paralelo
	workers := flag.Int("workers", 4, "cuantas peticiones atiende persistencia en paralelo")
	// escalabilidad de lectura, cadena de la replica opcional
	dsnLectura := flag.String("dsn-lectura", os.Getenv("DATABASE_URL_LECTURA"), "cadena de conexion a una replica de solo lectura (opcional)")
	flag.Parse()
	if *workers < 1 {
		*workers = 1
	}

	log.SetOutput(os.Stdout)
	log.SetPrefix("persistencia ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	if *dsn == "" {
		log.Fatal("no hay a donde conectarse, falta DATABASE_URL en el .env o el flag -dsn")
	}
	// ESCALABILIDAD VERTICAL, una conexion por worker y una extra para imprimir la tabla
	conexiones := int32(*workers + 1)
	// aqui se crea el pool de conexiones a supabase y se aplica schema.sql
	base, err := db.Conectar(*dsn, conexiones)
	if err != nil {
		log.Fatal(err)
	}
	defer base.Cerrar()
	// si hay replica se usa para las consultas de eventos
	if *dsnLectura != "" {
		// aca se crea la conexion a la replica de lectura
		if err := base.UsarReplica(*dsnLectura, conexiones); err != nil {
			log.Fatal(err)
		}
		log.Printf("las consultas de eventos van a la replica de lectura")
	}

	// con -ver solo muestra el estado de la base y sale, nunca borra nada
	if *ver {
		mostrarTabla(base, *limite)
		return
	}
	// con -inicial o -generar se borra la base y se carga el estado inicial, solo en la m3
	if *generar || *inicial != "" {
		var iniciales []model.Evento
		// aca se crean los eventos, inventados o leidos del json
		if *generar {
			iniciales, err = db.GenerarEventos(*mes, *eventos, *dobles, *asientos)
		} else {
			iniciales, err = db.LeerEventos(*inicial)
		}
		if err == nil {
			err = base.Reiniciar(context.Background(), iniciales)
		}
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
	defer frontend.Close()
	if err := frontend.Bind(*direccion); err != nil {
		log.Fatalf("no se pudo abrir %s: %v", *direccion, err)
	}
	// ESCALABILIDAD VERTICAL, aca se crea el dealer interno que reparte peticiones entre los workers
	backend, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		log.Fatal(err)
	}
	defer backend.Close()
	if err := backend.Bind("inproc://workers"); err != nil {
		log.Fatal(err)
	}

	// aca se crea el canal de avisos para reimprimir la tabla, con espacio para uno
	avisos := make(chan struct{}, 1)
	if *tabla {
		go imprimirTabla(base, *limite, avisos)
	}
	// ESCALABILIDAD VERTICAL, aca se crean los workers, cada uno en su propia goroutine
	for i := 1; i <= *workers; i++ {
		go trabajador(i, base, *limite, avisos) // cada worker se conecta al dealer interno
	}
	log.Printf("listo, escuchando en %s con %d workers", *direccion, *workers)
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
	defer socket.Close()
	// se conecta al dealer interno
	if err := socket.Connect("inproc://workers"); err != nil {
		log.Fatal(err)
	}
	for {
		mensaje, err := socket.RecvBytes(0)
		if err != nil {
			log.Printf("worker %d no pudo recibir: %v", n, err)
			continue
		}
		ctx, cancelar := context.WithTimeout(context.Background(), limite)
		respuesta, cambio := atender(ctx, base, mensaje)
		cancelar()
		datos, _ := json.Marshal(respuesta)
		// un socket rep siempre tiene que responder
		if _, err := socket.SendBytes(datos, 0); err != nil {
			log.Printf("worker %d no pudo responder: %v", n, err)
		}
		if cambio {
			select {
			case avisos <- struct{}{}:
			default:
			}
		}
	}
}

// imprimirTabla reimprime la base cada vez que llega un aviso
func imprimirTabla(base *db.BaseDatos, limite time.Duration, avisos chan struct{}) {
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
	s := peticion.Solicitud
	var resp model.Respuesta
	var err error
	switch peticion.Operacion {
	case model.OpConsultarEventos:
		var eventos []model.Evento
		eventos, err = base.ConsultarEventos(ctx, s.Mes)
		encontrados := fmt.Sprintf("se encontraron %d eventos en %s", len(eventos), s.Mes)
		if len(eventos) == 1 {
			encontrados = fmt.Sprintf("se encontro 1 evento en %s", s.Mes)
		}
		// aqui se crea la respuesta con los eventos
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Exito: true, Mensaje: encontrados, Eventos: eventos}
	case model.OpConsultarDisponibilidad:
		// TOLERANCIA A FALLAS, si otro rmc ya hizo esta reserva antes de caerse se devuelve lo guardado
		if guardada, ya, errYa := base.YaProcesada(ctx, s.IdSolicitud); errYa == nil && ya {
			resp = guardada
			break
		}
		var disponibles int
		disponibles, err = base.Disponibilidad(ctx, s.IdEvento, s.IdOcurrencia)
		// aqui se crea la respuesta con los asientos libres
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Exito: true, Mensaje: fmt.Sprintf("%s en la fecha %d tiene %d asientos libres", s.IdEvento, s.IdOcurrencia, disponibles), AsientosDisponibles: disponibles}
	// aqui se crea la reserva en la base
	case model.OpCrearReserva:
		resp, err = base.CrearReserva(ctx, s)
		// cambia los asientos o la fecha de una reserva
	case model.OpModificarReserva:
		resp, err = base.ModificarReserva(ctx, s)
	// libera los asientos de una reserva
	case model.OpCancelarReserva:
		resp, err = base.CancelarReserva(ctx, s)
	default:
		err = fmt.Errorf("no se reconoce la operacion %q", peticion.Operacion)
	}
	if err != nil {
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: err.Error()}
	}
	log.Printf("%s (%s), %s: %s", s.Resumen(), queHace(peticion.Operacion), model.Resultado(resp.Exito), resp.Mensaje)
	escribe := peticion.Operacion == model.OpCrearReserva || peticion.Operacion == model.OpModificarReserva || peticion.Operacion == model.OpCancelarReserva
	cambio := resp.Exito && escribe
	return resp, cambio
}

// mostrarTabla imprime los eventos con sus asientos y la lista de reservas
func mostrarTabla(base *db.BaseDatos, limite time.Duration) {
	ctx, cancelar := context.WithTimeout(context.Background(), limite)
	defer cancelar()
	tabla, err := base.Tabla(ctx, true)
	if err != nil {
		log.Printf("no se pudo leer la base: %v", err)
		return
	}
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
	
	case model.OpModificarReserva:
		return "modifica la reserva"
	case model.OpCancelarReserva:
		return "libera los asientos"
	}
	return string(op)
}
