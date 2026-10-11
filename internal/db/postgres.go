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

//go:embed schema.sql
var esquema string

type BaseDatos struct {
	pool *pgxpool.Pool
}

func Conectar(dsn string) (*BaseDatos, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("la cadena de conexion no es valida: %w", err)
	}
	config.MaxConns = 4
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

	if _, err := pool.Exec(ctx, esquema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("no se pudo aplicar el esquema: %w", err)
	}
	return &BaseDatos{pool: pool}, nil
}

func (b *BaseDatos) Cerrar() {
	b.pool.Close()
}

func (b *BaseDatos) Reiniciar(ctx context.Context, eventos []model.Evento) error {
	return pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `TRUNCATE srb.solicitudes_procesadas, srb.reservas, srb.ocurrencias, srb.events`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `ALTER SEQUENCE srb.reservas_seq RESTART`); err != nil {
			return err
		}
		var filasEventos, filasOcurrencias [][]any
		for i, evento := range eventos {
			filasEventos = append(filasEventos, []any{evento.IdEvento, evento.Nombre, i + 1})
			for _, oc := range evento.Ocurrencias {
				fecha, err := time.Parse("2006-01-02", oc.Fecha)
				if err != nil {
					return fmt.Errorf("el evento %s trae una fecha que no se entiende: %q", evento.IdEvento, oc.Fecha)
				}
				filasOcurrencias = append(filasOcurrencias, []any{evento.IdEvento, oc.IdOcurrencia, fecha, oc.CapacidadTotal, oc.AsientosDisponibles, oc.AsientosVendidos})
			}
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"srb", "events"}, []string{"id_evento", "nombre_evento", "posicion"}, pgx.CopyFromRows(filasEventos)); err != nil {
			return err
		}
		_, err := tx.CopyFrom(ctx, pgx.Identifier{"srb", "ocurrencias"},
			[]string{"id_evento", "id_ocurrencia", "fecha", "capacidad_total", "asientos_disponibles", "asientos_vendidos"},
			pgx.CopyFromRows(filasOcurrencias)) // carga masiva en un solo viaje a la BD
		return err
	})
}

func (b *BaseDatos) ConsultarEventos(ctx context.Context, mes string) ([]model.Evento, error) {
	inicio, err := time.Parse("2006-01", mes)
	if err != nil {
		return nil, fmt.Errorf("el mes %q no sirve, escribalo como 2026-11", mes)
	}
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

func (b *BaseDatos) Disponibilidad(ctx context.Context, idEvento string, idOcurrencia int) (int, error) {
	var disponibles int
	err := b.pool.QueryRow(ctx, `SELECT asientos_disponibles FROM srb.ocurrencias WHERE id_evento = $1 AND id_ocurrencia = $2`, idEvento, idOcurrencia).Scan(&disponibles)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("no existe %s con fecha %d", idEvento, idOcurrencia)
	}
	return disponibles, err
}

func (b *BaseDatos) CrearReserva(ctx context.Context, s model.Solicitud) (model.Respuesta, error) {
	return b.llamar(ctx, `SELECT srb.crear_reserva($1, $2, $3, $4, $5)`, s.IdSolicitud, s.IdCliente, s.IdEvento, s.IdOcurrencia, s.CantidadAsientos)
}

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
		SELECT r.id_reserva, r.id_cliente, r.id_evento, r.cantidad_asientos, o.fecha
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
		if err := filasReservas.Scan(&r.IdReserva, &r.IdCliente, &r.IdEvento, &r.CantidadAsientos, &fecha); err != nil {
			return "", err
		}
		palabra := "asientos"
		if r.CantidadAsientos == 1 {
			palabra = "asiento"
		}
		fmt.Fprintf(&sb, "   - %s: %s reservo %d %s de %s para el %s\n", r.IdReserva, r.IdCliente, r.CantidadAsientos, palabra, r.IdEvento, model.FechaLegible(fecha.Format("2006-01-02")))
	}
	return sb.String(), filasReservas.Err()
}

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
		if len(eventos) == 0 || eventos[len(eventos)-1].IdEvento != idEvento {
			eventos = append(eventos, model.Evento{IdEvento: idEvento, Nombre: nombre})
		}
		ultimo := &eventos[len(eventos)-1]
		ultimo.Ocurrencias = append(ultimo.Ocurrencias, oc)
	}
	return eventos, filas.Err()
}
