package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	zmq "github.com/pebbe/zmq4"

	"sistemaboletos/internal/db"
	"sistemaboletos/internal/model"
)

type registroMetricas struct {
	archivo *os.File
	csv     *csv.Writer
}

func main() {
	direccion := flag.String("bind", "tcp://*:5560", "endpoint REP donde escucha el servicio")
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "conexión a PostgreSQL/Supabase (por defecto $DATABASE_URL)")
	inicial := flag.String("inicial", "", "archivo JSON con el estado inicial (borra y recarga la BD)")
	generar := flag.Bool("generar", false, "genera eventos automáticamente (borra y recarga la BD)")
	mes := flag.String("mes", "2026-11", "mes de los eventos generados (AAAA-MM)")
	eventos := flag.Int("eventos", 50, "cantidad de eventos a generar")
	dobles := flag.Int("dobles", 20, "cuántos eventos generados ocurren dos veces en el mes")
	asientos := flag.Int("asientos", 100, "asientos por ocurrencia")
	tabla := flag.Bool("tabla", true, "muestra la BD después de cada cambio")
	metricas := flag.String("metricas", "", "archivo CSV donde se registra cada operación (para calcular TPM)")
	ver := flag.Bool("ver", false, "solo muestra el contenido de la BD y termina")
	limite := flag.Duration("limite", 5*time.Second, "tiempo máximo de cada operación en la BD")
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetPrefix("[PERSISTENCIA] ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	if *dsn == "" {
		log.Fatal("falta la conexión a la BD: exporte DATABASE_URL o use -dsn")
	}
	base, err := db.Conectar(*dsn) // nunca se imprime el DSN porque contiene la contraseña
	if err != nil {
		log.Fatal(err)
	}
	defer base.Cerrar()

	if *ver {
		mostrarTabla(base, *limite)
		return
	}
	if *generar || *inicial != "" {
		var iniciales []model.Evento
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
		log.Printf("BD reiniciada con %d eventos", len(iniciales))
	}

	registro, err := nuevoRegistro(*metricas)
	if err != nil {
		log.Fatalf("no se pudo crear el archivo de métricas: %v", err)
	}

	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	defer socket.Close()
	if err := socket.Bind(*direccion); err != nil {
		log.Fatalf("no se pudo abrir %s: %v", *direccion, err)
	}
	log.Printf("escuchando en %s | conectado a PostgreSQL", *direccion)
	fmt.Println("ESTADO INICIAL DE LA BD")
	mostrarTabla(base, *limite)

	for {
		mensaje, err := socket.RecvBytes(0)
		if err != nil {
			log.Printf("error recibiendo: %v", err)
			continue
		}
		ctx, cancelar := context.WithTimeout(context.Background(), *limite)
		respuesta, cambio := atender(ctx, base, mensaje, registro)
		cancelar()
		datos, _ := json.Marshal(respuesta)
		if _, err := socket.SendBytes(datos, 0); err != nil {
			log.Printf("error respondiendo: %v", err)
		}
		if *tabla && cambio {
			mostrarTabla(base, *limite) // se imprime después de responder para no sumar tiempo a la respuesta
		}
	}
}

func atender(ctx context.Context, base *db.BaseDatos, mensaje []byte, registro *registroMetricas) (model.Respuesta, bool) {
	var peticion model.PeticionDB
	if err := json.Unmarshal(mensaje, &peticion); err != nil {
		return model.Respuesta{Mensaje: "petición mal formada: " + err.Error()}, false
	}
	s := peticion.Solicitud
	var resp model.Respuesta
	var err error
	switch peticion.Operacion {
	case model.OpConsultarEventos:
		var eventos []model.Evento
		eventos, err = base.ConsultarEventos(ctx, s.Mes)
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Exito: true, Mensaje: fmt.Sprintf("%d eventos en %s", len(eventos), s.Mes), Eventos: eventos}
	case model.OpConsultarDisponibilidad:
		var disponibles int
		disponibles, err = base.Disponibilidad(ctx, s.IdEvento, s.IdOcurrencia)
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Exito: true, Mensaje: fmt.Sprintf("%s/%d tiene %d asientos disponibles", s.IdEvento, s.IdOcurrencia, disponibles), AsientosDisponibles: disponibles}
	case model.OpCrearReserva:
		resp, err = base.CrearReserva(ctx, s)
	case model.OpCancelarReserva:
		resp, err = base.CancelarReserva(ctx, s)
	default:
		err = fmt.Errorf("operación %q desconocida", peticion.Operacion)
	}
	if err != nil {
		resp = model.Respuesta{IdSolicitud: s.IdSolicitud, Mensaje: err.Error()} // cualquier error de la BD se responde como rechazo
	}
	log.Printf("%-24s %s -> %s %s", peticion.Operacion, s.Resumen(), model.Resultado(resp.Exito), resp.Mensaje)
	registro.anotar(peticion.Operacion, s, resp.Exito)
	cambio := resp.Exito && (peticion.Operacion == model.OpCrearReserva || peticion.Operacion == model.OpCancelarReserva)
	return resp, cambio
}

func mostrarTabla(base *db.BaseDatos, limite time.Duration) {
	ctx, cancelar := context.WithTimeout(context.Background(), limite)
	defer cancelar()
	tabla, err := base.Tabla(ctx, true)
	if err != nil {
		log.Printf("no se pudo leer la BD: %v", err)
		return
	}
	fmt.Print(tabla)
}

func nuevoRegistro(ruta string) (*registroMetricas, error) {
	if ruta == "" {
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(ruta), 0o755); err != nil {
		return nil, err
	}
	archivo, err := os.Create(ruta)
	if err != nil {
		return nil, err
	}
	escritor := csv.NewWriter(archivo)
	escritor.Write([]string{"marca_unix_ms", "operacion", "id_solicitud", "id_cliente", "exito"})
	escritor.Flush()
	return &registroMetricas{archivo: archivo, csv: escritor}, nil
}

func (r *registroMetricas) anotar(operacion model.OperacionDB, s model.Solicitud, exito bool) {
	if r == nil {
		return // métricas desactivadas
	}
	r.csv.Write([]string{strconv.FormatInt(time.Now().UnixMilli(), 10), string(operacion), s.IdSolicitud, s.IdCliente, strconv.FormatBool(exito)})
	r.csv.Flush()
}
