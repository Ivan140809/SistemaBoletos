// SERVICIO DE PERSISTENCIA - Máquina 3, capa de almacenamiento.
//
// Es el ÚNICO proceso que habla con la base de datos (PostgreSQL en Supabase).
// Escucha peticiones de RMC por un socket REP (puerto 5560), las traduce a
// operaciones sobre la base usando el paquete internal/db y devuelve el resultado.
//
//	RMC (M2) --(REQ/REP)--> [ persistencia ] --(SQL sobre internet)--> Supabase
//
// Además de atender peticiones, este proceso sirve para OBSERVAR la base durante
// la sustentación: imprime el estado inicial y vuelve a imprimir la tabla
// después de cada reserva exitosa.
//
// Modos de arranque (según las opciones):
//   - Normal (sin -inicial ni -generar): conserva lo que haya en la base.
//   - Con -inicial archivo.json o -generar: BORRA la base y la carga desde cero.
//     (scripts/machine3_storage.sh agrega "-inicial data/test_case.json" por defecto)
//   - Con -ver: solo imprime cómo está la base y termina, sin abrir el servicio.
package main

import (
	"context" // contextos: ponerle límite de tiempo a las operaciones de la base
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	zmq "github.com/pebbe/zmq4"

	"sistemaboletos/internal/db"
	"sistemaboletos/internal/model"
)

func main() {
	// Opciones de línea de comandos (todas son punteros: se leen con *).
	direccion := flag.String("bind", "tcp://*:5560", "endpoint REP donde escucha el servicio")
	// os.Getenv("DATABASE_URL") lee la variable de entorno. OJO: Go NO lee el
	// archivo .env por sí solo; por eso el script machine3_storage.sh hace
	// "source .env" antes de arrancar este programa.
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
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetPrefix("persistencia ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	if *dsn == "" {
		log.Fatal("no hay a donde conectarse, falta DATABASE_URL en el .env o el flag -dsn")
	}
	// Abre el pool de conexiones a Supabase y aplica schema.sql (crea tablas si faltan).
	base, err := db.Conectar(*dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer base.Cerrar()

	// Modo -ver: imprime la base y sale. "return" dentro de main termina el programa.
	// Fíjese que esto ocurre ANTES del reinicio: -ver nunca borra nada.
	if *ver {
		mostrarTabla(base, *limite)
		return
	}
	// Si pidieron estado inicial (por archivo o generado), se reinicia la base.
	if *generar || *inicial != "" {
		var iniciales []model.Evento
		if *generar {
			iniciales, err = db.GenerarEventos(*mes, *eventos, *dobles, *asientos)
		} else {
			iniciales, err = db.LeerEventos(*inicial)
		}
		if err == nil {
			// Borra las tablas y carga los eventos (en una sola transacción).
			err = base.Reiniciar(context.Background(), iniciales)
		}
		if err != nil {
			log.Fatalf("no se pudo cargar el estado inicial: %v", err)
		}
		log.Printf("base reiniciada con %d eventos", len(iniciales))
	}

	// Socket REP: el servidor del patrón request/reply. Hace Bind (se queda
	// escuchando en el puerto 5560 de esta máquina) y RMC se conecta a él.
	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	defer socket.Close()
	if err := socket.Bind(*direccion); err != nil {
		log.Fatalf("no se pudo abrir %s: %v", *direccion, err)
	}
	log.Printf("listo, escuchando en %s", *direccion)
	fmt.Println("estado inicial de la base")
	mostrarTabla(base, *limite)

	// Ciclo principal: recibe una petición, la atiende, responde y, si hubo
	// cambios en la base, imprime la tabla. Atiende de a una a la vez (un solo hilo),
	// lo que además serializa los accesos a la base desde esta capa.
	for {
		mensaje, err := socket.RecvBytes(0)
		if err != nil {
			log.Printf("no se pudo recibir: %v", err)
			continue
		}
		// Cada operación sobre la base tiene un tiempo máximo ("limite"); si se
		// pasa, se cancela y se devuelve error.
		ctx, cancelar := context.WithTimeout(context.Background(), *limite)
		respuesta, cambio := atender(ctx, base, mensaje)
		cancelar()
		datos, _ := json.Marshal(respuesta)
		// Siempre se responde (regla de los sockets REP).
		if _, err := socket.SendBytes(datos, 0); err != nil {
			log.Printf("no se pudo responder: %v", err)
		}
		if *tabla && cambio {
			mostrarTabla(base, *limite) // se imprime después de responder para no sumar tiempo a la respuesta
		}
	}
}

// atender interpreta una petición de RMC, ejecuta la operación en la base y
// devuelve (respuesta, huboCambio). "huboCambio" es true solo cuando se creó una
// reserva con éxito; sirve para decidir si hay que reimprimir la tabla.
func atender(ctx context.Context, base *db.BaseDatos, mensaje []byte) (model.Respuesta, bool) {
	var peticion model.PeticionDB
	if err := json.Unmarshal(mensaje, &peticion); err != nil {
		return model.Respuesta{Mensaje: "no se pudo leer la peticion: " + err.Error()}, false
	}
	s := peticion.Solicitud
	var resp model.Respuesta
	var err error
	// Según la operación pedida por RMC se llama al método correspondiente de db.
	switch peticion.Operacion {
	case model.OpConsultarEventos:
		var eventos []model.Evento
		eventos, err = base.ConsultarEventos(ctx, s.Mes)
		encontrados := fmt.Sprintf("se encontraron %d eventos en %s", len(eventos), s.Mes)
		if len(eventos) == 1 {
			encontrados = fmt.Sprintf("se encontro 1 evento en %s", s.Mes)
		}
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Exito: true, Mensaje: encontrados, Eventos: eventos}
	case model.OpConsultarDisponibilidad:
		var disponibles int
		disponibles, err = base.Disponibilidad(ctx, s.IdEvento, s.IdOcurrencia)
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Exito: true, Mensaje: fmt.Sprintf("%s en la fecha %d tiene %d asientos libres", s.IdEvento, s.IdOcurrencia, disponibles), AsientosDisponibles: disponibles}
	case model.OpCrearReserva:
		resp, err = base.CrearReserva(ctx, s)
	default:
		err = fmt.Errorf("no se reconoce la operacion %q", peticion.Operacion)
	}
	// Si alguna operación falló (error de la base, evento inexistente, etc.), se
	// descarta lo que se había armado y se responde con el error (Exito=false).
	if err != nil {
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: err.Error()}
	}
	log.Printf("%s (%s), %s: %s", s.Resumen(), queHace(peticion.Operacion), model.Resultado(resp.Exito), resp.Mensaje)
	cambio := resp.Exito && peticion.Operacion == model.OpCrearReserva
	return resp, cambio
}

// mostrarTabla imprime en la terminal el estado completo de la base:
// eventos con sus asientos libres/vendidos y la lista de reservas.
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

// queHace traduce el nombre técnico de la operación a una frase corta para el log.
func queHace(op model.OperacionDB) string {
	switch op {
	case model.OpConsultarEventos:
		return "busca eventos"
	case model.OpConsultarDisponibilidad:
		return "revisa cupos"
	case model.OpCrearReserva:
		return "guarda la reserva"
	}
	return string(op)
}
