// paquete db: todo el acceso a la base de datos postgresql (en supabase).
//
// solo lo usa el proceso persistence (cmd/persistence). ningun otro servicio
// habla con la base, y eso es lo que vuelve a la persistencia una capa aparte
// (el "manejo centralizado de transacciones" del enunciado).
//
// conceptos de bases de datos que aparecen aqui:
//   - pool de conexiones: en vez de abrir una conexion nueva por consulta (que es
//     lento), se mantiene un grupo de conexiones abiertas y se van reutilizando.
//   - consulta parametrizada: en el sql se escribe $1, $2... y los valores se
//     pasan aparte. evita la inyeccion sql y es lo correcto siempre.
//   - transaccion: un grupo de operaciones que se aplican todas o ninguna.
package db

import (
	"context"
	_ "embed" // el "_" importa el paquete solo por su efecto (habilita //go:embed)
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"         // driver de postgresql para go ("pgx")
	"github.com/jackc/pgx/v5/pgxpool" // el pool de conexiones de pgx
	"sistemaboletos/internal/model"
	"strings"
	"time"
)

// "//go:embed" no es un comentario normal, es una directiva del compilador. mete
// el contenido de schema.sql dentro del ejecutable, como un string en la variable
// "esquema" que esta justo debajo. asi el programa carga su propio sql de
// creacion de tablas y no depende de que el archivo exista en el disco.
//
//go:embed schema.sql
var esquema string

// basedatos envuelve el pool de conexiones. el resto del sistema usa los metodos
// de este struct y nunca toca sql directamente.
type BaseDatos struct {
	pool *pgxpool.Pool
}

// conectar abre el pool contra supabase y deja las tablas listas.
// "dsn" es la cadena de conexion (la variable database_url del .env), con
// usuario, clave, host y puerto.
func Conectar(dsn string) (*BaseDatos, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("la cadena de conexion no es valida: %w", err)
	}
	// maximo 4 conexiones a la vez (supabase limita las conexiones en el plan gratis).
	config.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}
	// "context" es la forma que tiene go de ponerle limite de tiempo (o cancelar)
	// una operacion. withtimeout crea un contexto que se vence a los 30 segundos y
	// "cancelar" lo libera (por eso el defer).
	ctx, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelar()
	// ping es la prueba real de que la conexion funciona (credenciales, red, ssl).
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("no se pudo conectar a PostgreSQL: %w", err)
	}

	// corre todo schema.sql. como usa create ... if not exists / create or
	// replace, se puede correr siempre sin danar nada: crea lo que falte y no toca
	// los datos que ya existen.
	if _, err := pool.Exec(ctx, esquema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("no se pudo aplicar el esquema: %w", err)
	}
	return &BaseDatos{pool: pool}, nil
}

// cerrar cierra todas las conexiones del pool.
func (b *BaseDatos) Cerrar() {
	b.pool.Close()
}

// reiniciar borra todos los datos y carga los eventos que le pasen. es lo que deja
// la base "como nueva" cada vez que se arranca persistencia con -inicial o -generar.
// no hay una copia aparte de la base: el estado inicial vive en el json
// (data/test_case.json) y aqui se vuelve a escribir en las tablas.
//
// todo pasa dentro de una sola transaccion: o se completa entera o no cambia nada.
func (b *BaseDatos) Reiniciar(ctx context.Context, eventos []model.Evento) error {
	// beginfunc abre la transaccion, corre la funcion que le pasamos y hace
	// commit si devuelve nil o rollback si devuelve un error.
	// "func(tx pgx.tx) error { ... }" es una funcion anonima (una lambda).
	return pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		// 1) vaciar las 4 tablas (truncate borra todas las filas de una).
		if _, err := tx.Exec(ctx, `TRUNCATE srb.solicitudes_procesadas, srb.reservas, srb.ocurrencias, srb.events`); err != nil {
			return err
		}
		// 2) reiniciar el contador que numera las reservas, para que la primera
		// vuelva a ser r0001.
		if _, err := tx.Exec(ctx, `ALTER SEQUENCE srb.reservas_seq RESTART`); err != nil {
			return err
		}
		// 3) pasar los eventos de go a "filas" (listas de valores) para cargarlas
		// en bloque. [][]any es una lista de filas y cada fila una lista de valores.
		var filasEventos, filasOcurrencias [][]any
		for i, evento := range eventos {
			// "posicion" = i+1 guarda el orden original del archivo.
			filasEventos = append(filasEventos, []any{evento.IdEvento, evento.Nombre, i + 1})
			for _, oc := range evento.Ocurrencias {
				fecha, err := time.Parse("2006-01-02", oc.Fecha)
				if err != nil {
					return fmt.Errorf("el evento %s trae una fecha que no se entiende: %q", evento.IdEvento, oc.Fecha)
				}
				filasOcurrencias = append(filasOcurrencias, []any{evento.IdEvento, oc.IdOcurrencia, fecha, oc.CapacidadTotal, oc.AsientosDisponibles, oc.AsientosVendidos})
			}
		}
		// 4) cargar. copyfrom usa el comando copy de postgresql, que mete muchas
		// filas en un solo viaje. primero events (la tabla "padre") y despues
		// ocurrencias (la "hija", que referencia a events).
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"srb", "events"}, []string{"id_evento", "nombre_evento", "posicion"}, pgx.CopyFromRows(filasEventos)); err != nil {
			return err
		}
		_, err := tx.CopyFrom(ctx, pgx.Identifier{"srb", "ocurrencias"},
			[]string{"id_evento", "id_ocurrencia", "fecha", "capacidad_total", "asientos_disponibles", "asientos_vendidos"},
			pgx.CopyFromRows(filasOcurrencias)) // carga masiva en un solo viaje a la bd
		return err
	})
}

// consultareventos devuelve los eventos con fechas dentro del mes dado (ej.
// "2026-11"), con sus asientos libres y vendidos.
func (b *BaseDatos) ConsultarEventos(ctx context.Context, mes string) ([]model.Evento, error) {
	// "2026-11" pasa a ser 1 de noviembre de 2026.
	inicio, err := time.Parse("2006-01", mes)
	if err != nil {
		return nil, fmt.Errorf("el mes %q no sirve, escribalo como 2026-11", mes)
	}
	// el rango del mes es [inicio, inicio + 1 mes): desde el dia 1 incluido hasta
	// el dia 1 del mes siguiente sin incluirlo. $1 y $2 se reemplazan por los dos
	// valores que van despues del sql.
	filas, err := b.pool.Query(ctx, `
	SELECT id_evento, nombre_evento, id_ocurrencia, fecha, capacidad_total, asientos_disponibles, asientos_vendidos
	FROM srb.vista_ocupacion
	WHERE fecha >= $1 AND fecha < $2
	ORDER BY posicion, id_ocurrencia`, inicio, inicio.AddDate(0, 1, 0))
	if err != nil {
		return nil, err
	}
	return agruparEventos(filas)
}

// disponibilidad devuelve cuantos asientos libres quedan en una ocurrencia de un evento.
func (b *BaseDatos) Disponibilidad(ctx context.Context, idEvento string, idOcurrencia int) (int, error) {
	var disponibles int
	// queryrow espera como mucho una fila y scan copia las columnas de esa fila
	// en las variables que le pasamos (por puntero).
	err := b.pool.QueryRow(ctx, `SELECT asientos_disponibles FROM srb.ocurrencias WHERE id_evento = $1 AND id_ocurrencia = $2`, idEvento, idOcurrencia).Scan(&disponibles)
	// si no hay filas, ese evento o esa fecha no existe. errors.is compara errores.
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("no existe %s con fecha %d", idEvento, idOcurrencia)
	}
	return disponibles, err
}

// crearreserva le deja todo el trabajo a la funcion sql srb.crear_reserva (esta
// en schema.sql). esa funcion corre dentro de la base como una sola transaccion:
// verifica, descuenta asientos y registra la reserva de forma atomica. por eso
// aqui no hay ningun "if alcanzan los asientos".
func (b *BaseDatos) CrearReserva(ctx context.Context, s model.Solicitud) (model.Respuesta, error) {
	return b.llamar(ctx, `SELECT srb.crear_reserva($1, $2, $3, $4, $5)`, s.IdSolicitud, s.IdCliente, s.IdEvento, s.IdOcurrencia, s.CantidadAsientos)
}

// llamar ejecuta una funcion sql que devuelve un json y lo pasa a una respuesta.
// "argumentos ...any" es "cualquier cantidad de argumentos".
func (b *BaseDatos) llamar(ctx context.Context, sql string, argumentos ...any) (model.Respuesta, error) {
	var datos []byte
	if err := b.pool.QueryRow(ctx, sql, argumentos...).Scan(&datos); err != nil { // la funcion corre como una transaccion completa
		return model.Respuesta{}, err
	}
	var resp model.Respuesta
	if err := json.Unmarshal(datos, &resp); err != nil {
		return model.Respuesta{}, fmt.Errorf("la base devolvio algo que no se entiende: %w", err)
	}
	return resp, nil
}

// tabla arma el texto con el estado completo de la base (los eventos y, si hay,
// la lista de reservas). es lo que se imprime en la terminal de persistencia y lo
// que muestra "./bin/persistence -ver". sirve para la sustentacion: el "estado de
// la bd antes y despues de cada operacion".
func (b *BaseDatos) Tabla(ctx context.Context, conReservas bool) (string, error) {
	filasEventos, err := b.pool.Query(ctx, `
		SELECT id_evento, nombre_evento, id_ocurrencia, fecha, capacidad_total, asientos_disponibles, asientos_vendidos
		FROM srb.vista_ocupacion
		ORDER BY posicion, id_ocurrencia`)
	if err != nil {
		return "", err
	}
	eventos, err := agruparEventos(filasEventos)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	sb.WriteString(model.TablaEventos(eventos))
	// cuenta cuantas reservas hay.
	var reservas int
	if err := b.pool.QueryRow(ctx, `SELECT count(*) FROM srb.reservas`).Scan(&reservas); err != nil {
		return "", err
	}
	if reservas == 0 {
		sb.WriteString("todavia no hay reservas\n")
		return sb.String(), nil
	}
	fmt.Fprintf(&sb, "reservas hechas: %d\n", reservas)
	if !conReservas {
		return sb.String(), nil
	}
	// lista de reservas, con un join a ocurrencias para sacar la fecha.
	filasReservas, err := b.pool.Query(ctx, `
		SELECT r.id_reserva, r.id_cliente, r.id_evento, r.cantidad_asientos, o.fecha
		FROM srb.reservas r
		JOIN srb.ocurrencias o ON o.id_evento = r.id_evento AND o.id_ocurrencia = r.id_ocurrencia
		ORDER BY r.creada_en, r.id_reserva`)
	if err != nil {
		return "", err
	}
	defer filasReservas.Close()
	// next() avanza a la siguiente fila (da false cuando ya no quedan).
	for filasReservas.Next() {
		var r model.Reserva
		var fecha time.Time
		if err := filasReservas.Scan(&r.IdReserva, &r.IdCliente, &r.IdEvento, &r.CantidadAsientos, &fecha); err != nil {
			return "", err
		}
		palabra := "asientos"
		if r.CantidadAsientos == 1 {
			palabra = "asiento"
		}
		fmt.Fprintf(&sb, "   - %s: %s reservo %d %s de %s para el %s\n", r.IdReserva, r.IdCliente, r.CantidadAsientos, palabra, r.IdEvento, model.FechaLegible(fecha.Format("2006-01-02")))
	}
	// err() dice si el recorrido termino por un error y no por llegar al final.
	return sb.String(), filasReservas.Err()
}

// agrupareventos pasa las filas planas de la base (una por ocurrencia) a la
// estructura anidada []evento (cada evento con su lista de ocurrencias).
// funciona porque las consultas vienen ordenadas por evento: mientras el
// id_evento no cambie, la fila es del mismo evento que la anterior.
func agruparEventos(filas pgx.Rows) ([]model.Evento, error) {
	defer filas.Close()
	var eventos []model.Evento
	for filas.Next() {
		var idEvento, nombre string
		var fecha time.Time
		var oc model.Ocurrencia
		// scan llena cada variable con la columna que le toca, en orden.
		if err := filas.Scan(&idEvento, &nombre, &oc.IdOcurrencia, &fecha, &oc.CapacidadTotal, &oc.AsientosDisponibles, &oc.AsientosVendidos); err != nil {
			return nil, err
		}
		oc.Fecha = fecha.Format("2006-01-02")
		// si es la primera fila, o el evento cambio respecto al ultimo, se crea uno nuevo.
		if len(eventos) == 0 || eventos[len(eventos)-1].IdEvento != idEvento {
			eventos = append(eventos, model.Evento{IdEvento: idEvento, Nombre: nombre})
		}
		// "&eventos[len(eventos)-1]" es un puntero al ultimo evento de la lista, asi
		// le podemos agregar la ocurrencia directo.
		ultimo := &eventos[len(eventos)-1]
		ultimo.Ocurrencias = append(ultimo.Ocurrencias, oc)
	}
	return eventos, filas.Err()
}
