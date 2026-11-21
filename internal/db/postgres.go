// paquete db, todo el acceso a la base postgresql en supabase y solo lo usa persistencia.
// usa un pool de conexiones que se reutilizan, consultas con $1 $2 para evitar inyeccion sql
// y transacciones para que las operaciones se apliquen todas o ninguna.
// puede usar una replica de solo lectura para las consultas de eventos y quitarle carga a la principal.
package db

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sistemaboletos/internal/model"
	"strings"
	"time"
)

// la directiva de abajo mete schema.sql dentro del ejecutable en la variable esquema
//go:embed schema.sql
var esquema string

// BaseDatos guarda los pools de conexiones y el resto del sistema solo usa sus metodos
type BaseDatos struct {
	pool *pgxpool.Pool // base principal, todas las escrituras y lo que necesita el dato mas reciente
	lectura *pgxpool.Pool // replica de solo lectura para consultar eventos, si no hay es la misma principal
}

// Conectar abre la base principal y deja las tablas listas, ESCALABILIDAD VERTICAL, conexiones debe ser al menos el numero de workers
func Conectar(dsn string, conexiones int32) (*BaseDatos, error) {
	// aqui se crea el pool de la base principal
	pool, err := abrirPool(dsn, conexiones)
	if err != nil {
		return nil, err
	}
	ctx, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelar()
	// aqui se crean las tablas que falten corriendo schema.sql sin tocar los datos
	if _, err := pool.Exec(ctx, esquema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("no se pudo aplicar el esquema: %w", err)
	}
	// aqui se crea la base lista, sin replica las lecturas usan la misma principal
	return &BaseDatos{pool: pool, lectura: pool}, nil
}

// UsarReplica conecta una replica de solo lectura para las consultas de eventos.
// la replica va un poco atrasada, por eso las reservas siempre se deciden en la principal
func (b *BaseDatos) UsarReplica(dsn string, conexiones int32) error {
	// aqui se crea el pool de la replica, sin esquema porque la replica copia todo de la principal
	lectura, err := abrirPool(dsn, conexiones)
	if err != nil {
		return fmt.Errorf("replica de lectura: %w", err)
	}
	// desde ahora las consultas de eventos van a la replica
	b.lectura = lectura
	return nil
}

// abrirPool crea un pool de conexiones y prueba que funcione
func abrirPool(dsn string, conexiones int32) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("la cadena de conexion no es valida: %w", err)
	}
	config.MaxConns = conexiones
	// aqui se crea el pool de conexiones
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}
	ctx, cancelar := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelar()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("no se pudo conectar a PostgreSQL: %w", err)
	}
	return pool, nil
}

// TieneReplica dice si las consultas van a una replica aparte
func (b *BaseDatos) TieneReplica() bool {
	return b.lectura != b.pool
}

// Cerrar cierra todas las conexiones de los pools
func (b *BaseDatos) Cerrar() {
	if b.TieneReplica() {
		b.lectura.Close()
	}
	b.pool.Close()
}

// Reiniciar borra todo y carga los eventos dados dentro de una sola transaccion
func (b *BaseDatos) Reiniciar(ctx context.Context, eventos []model.Evento) error {
	// aqui se crea la transaccion, hace commit si todo sale bien o rollback si falla
	return pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `TRUNCATE srb.solicitudes_procesadas, srb.reservas, srb.ocurrencias, srb.events`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `ALTER SEQUENCE srb.reservas_seq RESTART`); err != nil {
			return err
		}
		var filasEventos, filasOcurrencias [][]any
		for i, evento := range eventos {
			// aqui se crea la fila del evento con su posicion en el archivo
			filasEventos = append(filasEventos, []any{evento.IdEvento, evento.Nombre, i + 1})
			for _, oc := range evento.Ocurrencias {
				fecha, err := time.Parse("2006-01-02", oc.Fecha)
				if err != nil {
					return fmt.Errorf("el evento %s trae una fecha que no se entiende: %q", evento.IdEvento, oc.Fecha)
				}
				// aqui se crea la fila de la ocurrencia
				filasOcurrencias = append(filasOcurrencias, []any{evento.IdEvento, oc.IdOcurrencia, fecha, oc.CapacidadTotal, oc.AsientosDisponibles, oc.AsientosVendidos})
			}
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"srb", "events"}, []string{"id_evento", "nombre_evento", "posicion"}, pgx.CopyFromRows(filasEventos)); err != nil {
			return err
		}
		_, err := tx.CopyFrom(ctx, pgx.Identifier{"srb", "ocurrencias"},
			[]string{"id_evento", "id_ocurrencia", "fecha", "capacidad_total", "asientos_disponibles", "asientos_vendidos"},
			pgx.CopyFromRows(filasOcurrencias))
		return err
	})
}

// ConsultarEventos devuelve los eventos de un mes con sus asientos libres y vendidos
func (b *BaseDatos) ConsultarEventos(ctx context.Context, mes string) ([]model.Evento, error) {
	inicio, err := time.Parse("2006-01", mes)
	if err != nil {
		return nil, fmt.Errorf("el mes %q no sirve, escribalo como 2026-11", mes)
	}
	// escalabilidad de lectura, esta consulta va a la replica si existe y asi descarga la principal
	filas, err := b.lectura.Query(ctx, `
	SELECT id_evento, nombre_evento, id_ocurrencia, fecha, capacidad_total, asientos_disponibles, asientos_vendidos
	FROM srb.vista_ocupacion
	WHERE fecha >= $1 AND fecha < $2
	ORDER BY posicion, id_ocurrencia`, inicio, inicio.AddDate(0, 1, 0))
	if err != nil {
		return nil, err
	}
	return agruparEventos(filas)
}

// Disponibilidad devuelve cuantos asientos libres quedan en una fecha de un evento
func (b *BaseDatos) Disponibilidad(ctx context.Context, idEvento string, idOcurrencia int) (int, error) {
	var disponibles int
	err := b.pool.QueryRow(ctx, `SELECT asientos_disponibles FROM srb.ocurrencias WHERE id_evento = $1 AND id_ocurrencia = $2`, idEvento, idOcurrencia).Scan(&disponibles)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("no existe %s con fecha %d", idEvento, idOcurrencia)
	}
	return disponibles, err
}

// CrearReserva deja todo el trabajo a la funcion sql srb.crear_reserva que es atomica
func (b *BaseDatos) CrearReserva(ctx context.Context, s model.Solicitud) (model.Respuesta, error) {
	// aqui se crea la reserva dentro de la base
	return b.llamar(ctx, `SELECT srb.crear_reserva($1, $2, $3, $4, $5)`, s.IdSolicitud, s.IdCliente, s.IdEvento, s.IdOcurrencia, s.CantidadAsientos)
}

// ModificarReserva deja el trabajo a la funcion sql de modificar la reserva en la tramsaccion
func (b *BaseDatos) ModificarReserva(ctx context.Context, s model.Solicitud) (model.Respuesta, error) {
	// aqui se modifica la reserva dentro de la base
	return b.llamar(ctx, `SELECT srb.modificar_reserva($1, $2, $3, $4, $5)`, s.IdSolicitud, s.IdCliente, s.IdReserva, s.NuevaCantidad, s.NuevaOcurrencia)
}

// Cancelar reserva deja todo el trabajo a la funcion sql srb.CancelarReserva que librera los asientes y deja la reserva cancelada en una transaccion
func (b *BaseDatos) CancelarReserva(ctx context.Context, s model.Solicitud) (model.Respuesta, error) {
	// aqui se cancela la reserva dentro de la base
	return b.llamar(ctx, `SELECT srb.cancelar_reserva($1, $2, $3)`, s.IdSolicitud, s.IdCliente, s.IdReserva)
}
// llamar ejecuta una funcion sql que devuelve json y lo pasa a una respuesta
func (b *BaseDatos) llamar(ctx context.Context, sql string, argumentos ...any) (model.Respuesta, error) {
	var datos []byte
	// corre la funcion sql como una transaccion completa
	if err := b.pool.QueryRow(ctx, sql, argumentos...).Scan(&datos); err != nil {
		return model.Respuesta{}, err
	}
	// aqui se crea la respuesta a partir del json
	var resp model.Respuesta
	if err := json.Unmarshal(datos, &resp); err != nil {
		return model.Respuesta{}, fmt.Errorf("la base devolvio algo que no se entiende: %w", err)
	}
	return resp, nil
}

// Tabla arma el texto con el estado de la base que se imprime en persistencia
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
	filasReservas, err := b.pool.Query(ctx, `
		SELECT r.id_reserva, r.id_cliente, r.id_evento, r.cantidad_asientos, o.fecha, r.estado
		FROM srb.reservas r
		JOIN srb.ocurrencias o ON o.id_evento = r.id_evento AND o.id_ocurrencia = r.id_ocurrencia
		ORDER BY r.creada_en, r.id_reserva`)
	if err != nil {
		return "", err
	}
	defer filasReservas.Close()
	for filasReservas.Next() {
		var r model.Reserva
		var fecha time.Time
		if err := filasReservas.Scan(&r.IdReserva, &r.IdCliente, &r.IdEvento, &r.CantidadAsientos, &fecha, &r.Estado); err != nil {
			return "", err
		}
		palabra := "asientos"
		if r.CantidadAsientos == 1 {
			palabra = "asiento"
		}
		// las canceladas se marcan para que se vea el historial
		estado := ""
		if r.Estado == model.ReservaCancelada {
			estado = " (cancelada)"
		}
		// escribe la linea de la reserva
		fmt.Fprintf(&sb, "   - %s: %s reservo %d %s de %s para el %s%s\n", r.IdReserva, r.IdCliente, r.CantidadAsientos, palabra, r.IdEvento, model.FechaLegible(fecha.Format("2006-01-02")), estado)
	}
	return sb.String(), filasReservas.Err()
}

// agruparEventos pasa las filas planas de la base a eventos con su lista de fechas
func agruparEventos(filas pgx.Rows) ([]model.Evento, error) {
	defer filas.Close()
	var eventos []model.Evento
	for filas.Next() {
		var idEvento, nombre string
		var fecha time.Time
		var oc model.Ocurrencia
		if err := filas.Scan(&idEvento, &nombre, &oc.IdOcurrencia, &fecha, &oc.CapacidadTotal, &oc.AsientosDisponibles, &oc.AsientosVendidos); err != nil {
			return nil, err
		}
		oc.Fecha = fecha.Format("2006-01-02")
		// aqui se crea un evento nuevo si es la primera fila o cambio el evento
		if len(eventos) == 0 || eventos[len(eventos)-1].IdEvento != idEvento {
			eventos = append(eventos, model.Evento{IdEvento: idEvento, Nombre: nombre})
		}
		ultimo := &eventos[len(eventos)-1]
		ultimo.Ocurrencias = append(ultimo.Ocurrencias, oc)
	}
	return eventos, filas.Err()
}
