// servicio de persistencia, maquina 3 (capa de almacenamiento).
//
// es el unico proceso que habla con la base de datos (postgresql en supabase).
// escucha las peticiones de rmc por un socket rep (puerto 5560), las convierte en
// operaciones sobre la base usando internal/db y devuelve el resultado.
//
//	rmc (m2) --(req/rep)--> [ persistencia ] --(sql sobre internet)--> supabase
//
// ademas de atender peticiones, sirve para ver la base durante la sustentacion:
// imprime el estado inicial y vuelve a imprimir la tabla despues de cada reserva
// exitosa.
//
// modos de arranque segun las opciones:
//   - normal (sin -inicial ni -generar): deja lo que haya en la base.
//   - con -inicial archivo.json o -generar: borra la base y la carga desde cero.
//     (scripts/machine3_storage.sh agrega "-inicial data/test_case.json" por defecto)
//   - con -ver: solo imprime como esta la base y termina, sin abrir el servicio.
package main

import (
	"context" // para ponerle limite de tiempo a las operaciones de la base
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
	// opciones de la terminal (todas son punteros, se leen con *).
	direccion := flag.String("bind", "tcp://*:5560", "endpoint REP donde escucha el servicio")
	// os.getenv("database_url") lee la variable de entorno. ojo: go no lee el
	// archivo .env solo, por eso machine3_storage.sh hace "source .env" antes de
	// arrancar este programa.
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
	// abre el pool de conexiones a supabase y aplica schema.sql (crea las tablas
	// que falten).
	base, err := db.Conectar(*dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer base.Cerrar()

	// modo -ver: imprime la base y sale. "return" dentro de main termina el
	// programa. fijate que esto pasa antes del reinicio, asi que -ver nunca borra nada.
	if *ver {
		mostrarTabla(base, *limite)
		return
	}
	// si pidieron estado inicial (por archivo o generado), se reinicia la base.
	if *generar || *inicial != "" {
		var iniciales []model.Evento
		if *generar {
			iniciales, err = db.GenerarEventos(*mes, *eventos, *dobles, *asientos)
		} else {
			iniciales, err = db.LeerEventos(*inicial)
		}
		if err == nil {
			// borra las tablas y carga los eventos, todo en una transaccion.
			err = base.Reiniciar(context.Background(), iniciales)
		}
		if err != nil {
			log.Fatalf("no se pudo cargar el estado inicial: %v", err)
		}
		log.Printf("base reiniciada con %d eventos", len(iniciales))
	}

	// socket rep: el servidor de request/reply. hace bind (se queda escuchando en
	// el puerto 5560 de esta maquina) y rmc se conecta a el.
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

	// ciclo principal: recibe una peticion, la atiende, responde y, si hubo
	// cambios en la base, imprime la tabla. atiende de a una (un solo hilo), lo que
	// de paso ordena los accesos a la base desde esta capa.
	for {
		mensaje, err := socket.RecvBytes(0)
		if err != nil {
			log.Printf("no se pudo recibir: %v", err)
			continue
		}
		// cada operacion sobre la base tiene un tiempo maximo ("limite"). si se
		// pasa, se cancela y devuelve error.
		ctx, cancelar := context.WithTimeout(context.Background(), *limite)
		respuesta, cambio := atender(ctx, base, mensaje)
		cancelar()
		datos, _ := json.Marshal(respuesta)
		// siempre se responde (regla de los sockets rep).
		if _, err := socket.SendBytes(datos, 0); err != nil {
			log.Printf("no se pudo responder: %v", err)
		}
		if *tabla && cambio {
			mostrarTabla(base, *limite) // se imprime despues de responder para no sumarle tiempo a la respuesta
		}
	}
}

// atender lee una peticion de rmc, hace la operacion en la base y devuelve
// (respuesta, hubocambio). hubocambio es true solo cuando se creo una reserva con
// exito, y sirve para decidir si hay que reimprimir la tabla.
func atender(ctx context.Context, base *db.BaseDatos, mensaje []byte) (model.Respuesta, bool) {
	var peticion model.PeticionDB
	if err := json.Unmarshal(mensaje, &peticion); err != nil {
		return model.Respuesta{Mensaje: "no se pudo leer la peticion: " + err.Error()}, false
	}
	s := peticion.Solicitud
	var resp model.Respuesta
	var err error
	// segun la operacion que pidio rmc se llama al metodo que toca de db.
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
	// si algo fallo (error de la base, evento que no existe, etc.), se tira lo que
	// se habia armado y se responde con el error (exito = false).
	if err != nil {
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: err.Error()}
	}
	log.Printf("%s (%s), %s: %s", s.Resumen(), queHace(peticion.Operacion), model.Resultado(resp.Exito), resp.Mensaje)
	cambio := resp.Exito && peticion.Operacion == model.OpCrearReserva
	return resp, cambio
}

// mostrartabla imprime en la terminal el estado completo de la base: los eventos
// con sus asientos libres y vendidos, y la lista de reservas.
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

// quehace pasa el nombre tecnico de la operacion a una frase corta para el log.
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
