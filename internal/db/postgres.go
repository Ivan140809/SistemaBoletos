// Paquete db: todo el acceso a la base de datos PostgreSQL (alojada en Supabase).
//
// Solo el proceso "persistence" (cmd/persistence) usa este paquete. Ningún otro
// servicio habla con la base de datos: eso es lo que hace a la persistencia
// una capa aparte ("manejo centralizado de transacciones" del enunciado).
//
// Conceptos de base de datos que aparecen aquí:
//   - Pool de conexiones: en vez de abrir una conexión nueva por consulta (lento),
//     se mantiene un grupo de conexiones abiertas y se van reutilizando.
//   - Consulta parametrizada: en el SQL se escribe $1, $2... y los valores se
//     pasan aparte. Evita la inyección SQL y es lo correcto siempre.
//   - Transacción: un grupo de operaciones que se aplican todas o ninguna.
package db

import (
	"context"
	_ "embed" // el "_" importa el paquete solo por su efecto (habilita //go:embed)
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"         // driver de PostgreSQL para Go ("pgx")
	"github.com/jackc/pgx/v5/pgxpool" // el pool de conexiones de pgx
	"sistemaboletos/internal/model"
	"strings"
	"time"
)

// "//go:embed" NO es un comentario normal: es una directiva del compilador.
// Mete el contenido del archivo schema.sql dentro del ejecutable, como un string
// en la variable "esquema" que está justo debajo. Así el programa lleva su
// propio SQL de creación de tablas y no depende de que el archivo exista en disco.
//
//go:embed schema.sql
var esquema string

// BaseDatos envuelve el pool de conexiones. El resto del sistema usa los métodos
// de este struct y nunca toca SQL directamente.
type BaseDatos struct {
	pool *pgxpool.Pool
}

// Conectar abre el pool contra Supabase y deja las tablas listas.
// "dsn" es la cadena de conexión (la variable DATABASE_URL del archivo .env),
// con usuario, contraseña, host y puerto.
func Conectar(dsn string) (*BaseDatos, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("la cadena de conexion no es valida: %w", err)
	}
	// Máximo 4 conexiones simultáneas a la base (Supabase limita las conexiones
	// en el plan gratuito).
	config.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}
	// "context" es la forma de Go de ponerle límite de tiempo (o cancelar) una
	// operación. WithTimeout crea un contexto que se vence a los 30 segundos;
	// "cancelar" es la función que lo libera (por eso el defer).
	ctx, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelar()
	// Ping: prueba real de que la conexión funciona (credenciales, red, SSL).
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("no se pudo conectar a PostgreSQL: %w", err)
	}

	// Ejecuta todo schema.sql. Como usa CREATE ... IF NOT EXISTS / CREATE OR
	// REPLACE, se puede correr cada vez sin dañar nada: crea lo que falte y no
	// toca los datos existentes.
	if _, err := pool.Exec(ctx, esquema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("no se pudo aplicar el esquema: %w", err)
	}
	return &BaseDatos{pool: pool}, nil
}

// Cerrar cierra todas las conexiones del pool.
func (b *BaseDatos) Cerrar() {
	b.pool.Close()
}

// Reiniciar BORRA todos los datos y carga los eventos dados. Es lo que deja la
// base "como nueva" cada vez que se arranca persistencia con -inicial o -generar.
// No existe una copia aparte de la base: el estado inicial vive en el archivo
// JSON (data/test_case.json) y aquí se vuelve a escribir en las tablas.
//
// Todo ocurre dentro de UNA transacción: o se completa entera, o no cambia nada.
func (b *BaseDatos) Reiniciar(ctx context.Context, eventos []model.Evento) error {
	// BeginFunc abre la transacción, ejecuta la función que le pasamos y hace
	// COMMIT si devuelve nil o ROLLBACK si devuelve un error.
	// "func(tx pgx.Tx) error { ... }" es una función anónima (una lambda).
	return pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		// 1) Vaciar las 4 tablas (TRUNCATE borra todas las filas de golpe).
		if _, err := tx.Exec(ctx, `TRUNCATE srb.solicitudes_procesadas, srb.reservas, srb.ocurrencias, srb.events`); err != nil {
			return err
		}
		// 2) Reiniciar el contador que numera las reservas (para que la primera
		// vuelva a ser R0001).
		if _, err := tx.Exec(ctx, `ALTER SEQUENCE srb.reservas_seq RESTART`); err != nil {
			return err
		}
		// 3) Convertir los eventos de Go en "filas" (listas de valores) para
		// cargarlas en bloque. [][]any = lista de filas, cada fila una lista de valores.
		var filasEventos, filasOcurrencias [][]any
		for i, evento := range eventos {
			// "posicion" = i+1 conserva el orden original del archivo.
			filasEventos = append(filasEventos, []any{evento.IdEvento, evento.Nombre, i + 1})
			for _, oc := range evento.Ocurrencias {
				fecha, err := time.Parse("2006-01-02", oc.Fecha)
				if err != nil {
					return fmt.Errorf("el evento %s trae una fecha que no se entiende: %q", evento.IdEvento, oc.Fecha)
				}
				filasOcurrencias = append(filasOcurrencias, []any{evento.IdEvento, oc.IdOcurrencia, fecha, oc.CapacidadTotal, oc.AsientosDisponibles, oc.AsientosVendidos})
			}
		}
		// 4) Cargar. CopyFrom usa el comando COPY de PostgreSQL: inserta muchas
		// filas en un solo viaje. Primero events (tabla "padre") y luego
		// ocurrencias (tabla "hija" que referencia a events).
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"srb", "events"}, []string{"id_evento", "nombre_evento", "posicion"}, pgx.CopyFromRows(filasEventos)); err != nil {
			return err
		}
		_, err := tx.CopyFrom(ctx, pgx.Identifier{"srb", "ocurrencias"},
			[]string{"id_evento", "id_ocurrencia", "fecha", "capacidad_total", "asientos_disponibles", "asientos_vendidos"},
			pgx.CopyFromRows(filasOcurrencias)) // carga masiva en un solo viaje a la BD
		return err
	})
}

// ConsultarEventos devuelve los eventos cuyas fechas caen dentro del mes dado
// (ej. "2026-11"), con sus asientos libres y vendidos.
func (b *BaseDatos) ConsultarEventos(ctx context.Context, mes string) ([]model.Evento, error) {
	// "2026-11" -> 1 de noviembre de 2026.
	inicio, err := time.Parse("2006-01", mes)
	if err != nil {
		return nil, fmt.Errorf("el mes %q no sirve, escribalo como 2026-11", mes)
	}
	// El rango del mes es [inicio, inicio + 1 mes): desde el día 1 inclusive
	// hasta el día 1 del mes siguiente SIN incluirlo. $1 y $2 se reemplazan por
	// los dos valores que van después del SQL.
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

// Disponibilidad devuelve cuántos asientos libres quedan en UNA ocurrencia
// de un evento.
func (b *BaseDatos) Disponibilidad(ctx context.Context, idEvento string, idOcurrencia int) (int, error) {
	var disponibles int
	// QueryRow espera a lo sumo UNA fila; Scan copia las columnas de esa fila
	// en las variables que le pasamos (por puntero).
	err := b.pool.QueryRow(ctx, `SELECT asientos_disponibles FROM srb.ocurrencias WHERE id_evento = $1 AND id_ocurrencia = $2`, idEvento, idOcurrencia).Scan(&disponibles)
	// Si no hay filas, ese evento/fecha no existe. errors.Is compara tipos de error.
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("no existe %s con fecha %d", idEvento, idOcurrencia)
	}
	return disponibles, err
}

// CrearReserva delega TODO el trabajo a la función SQL srb.crear_reserva
// (está en schema.sql). Esa función corre dentro de la base como una sola
// transacción: verifica, descuenta asientos y registra la reserva de forma
// atómica. Por eso aquí no hay lógica de "if alcanzan los asientos".
func (b *BaseDatos) CrearReserva(ctx context.Context, s model.Solicitud) (model.Respuesta, error) {
	return b.llamar(ctx, `SELECT srb.crear_reserva($1, $2, $3, $4, $5)`, s.IdSolicitud, s.IdCliente, s.IdEvento, s.IdOcurrencia, s.CantidadAsientos)
}

// llamar ejecuta una función SQL que devuelve un JSON y lo convierte en una
// Respuesta. "argumentos ...any" significa "cualquier cantidad de argumentos".
func (b *BaseDatos) llamar(ctx context.Context, sql string, argumentos ...any) (model.Respuesta, error) {
	var datos []byte
	if err := b.pool.QueryRow(ctx, sql, argumentos...).Scan(&datos); err != nil { // la función corre como una transacción completa
		return model.Respuesta{}, err
	}
	var resp model.Respuesta
	if err := json.Unmarshal(datos, &resp); err != nil {
		return model.Respuesta{}, fmt.Errorf("la base devolvio algo que no se entiende: %w", err)
	}
	return resp, nil
}

// Tabla arma el texto con el estado completo de la base (eventos y, si hay,
// la lista de reservas). Es lo que se imprime en la terminal de persistencia
// y lo que muestra "./bin/persistence -ver". Sirve para la sustentación:
// "estado de la BD antes y después de cada operación".
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
	// Cuenta cuántas reservas existen.
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
	// Lista de reservas, haciendo JOIN con ocurrencias para mostrar la fecha.
	filasReservas, err := b.pool.Query(ctx, `
		SELECT r.id_reserva, r.id_cliente, r.id_evento, r.cantidad_asientos, o.fecha
		FROM srb.reservas r
		JOIN srb.ocurrencias o ON o.id_evento = r.id_evento AND o.id_ocurrencia = r.id_ocurrencia
		ORDER BY r.creada_en, r.id_reserva`)
	if err != nil {
		return "", err
	}
	defer filasReservas.Close()
	// Next() avanza a la siguiente fila (false cuando no quedan).
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
	// Err() revela si el recorrido terminó por un error y no por llegar al final.
	return sb.String(), filasReservas.Err()
}

// agruparEventos convierte filas planas de la base (una por cada ocurrencia)
// en la estructura anidada []Evento (cada evento con su lista de ocurrencias).
// Funciona porque las consultas vienen ORDENADAS por evento: mientras el
// id_evento no cambie, la fila pertenece al mismo evento que la anterior.
func agruparEventos(filas pgx.Rows) ([]model.Evento, error) {
	defer filas.Close()
	var eventos []model.Evento
	for filas.Next() {
		var idEvento, nombre string
		var fecha time.Time
		var oc model.Ocurrencia
		// Scan llena cada variable con la columna correspondiente, en orden.
		if err := filas.Scan(&idEvento, &nombre, &oc.IdOcurrencia, &fecha, &oc.CapacidadTotal, &oc.AsientosDisponibles, &oc.AsientosVendidos); err != nil {
			return nil, err
		}
		oc.Fecha = fecha.Format("2006-01-02")
		// Si es la primera fila, o el evento cambió respecto al último, se crea uno nuevo.
		if len(eventos) == 0 || eventos[len(eventos)-1].IdEvento != idEvento {
			eventos = append(eventos, model.Evento{IdEvento: idEvento, Nombre: nombre})
		}
		// "&eventos[len(eventos)-1]" = puntero al ÚLTIMO evento de la lista, para
		// poder agregarle la ocurrencia directamente.
		ultimo := &eventos[len(eventos)-1]
		ultimo.Ocurrencias = append(ultimo.Ocurrencias, oc)
	}
	return eventos, filas.Err()
}
