package model

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

type TipoSolicitud string

const (
	TipoConsulta TipoSolicitud = "consulta"
	TipoReserva TipoSolicitud = "reserva"
)

type OperacionDB string

const (
	OpConsultarEventos OperacionDB = "consultar_eventos"
	OpConsultarDisponibilidad OperacionDB = "consultar_disponibilidad"
	OpCrearReserva OperacionDB = "crear_reserva"
)
const (
	ReservaActiva = "ACTIVA"
	ReservaCancelada = "CANCELADA"
)

type Ocurrencia struct {
	IdOcurrencia int `json:"id_ocurrencia"`
	Fecha string `json:"fecha"`
	CapacidadTotal int `json:"capacidad_total"`
	AsientosDisponibles int `json:"asientos_disponibles"`
	AsientosVendidos int `json:"asientos_vendidos"`
}
type Evento struct {
	IdEvento string `json:"id_evento"`
	Nombre string `json:"nombre"`
	Ocurrencias []Ocurrencia `json:"ocurrencias"`
}

type Reserva struct {
	IdReserva string `json:"id_reserva"`
	IdCliente string `json:"id_cliente"`
	IdOcurrencia int `json:"id_ocurrencia"`
	IdEvento string `json:"id_evento"`
	CantidadAsientos int `json:"cantidad_asientos"`
	Estado string `json:"estado"`
	Creada string `json:"creada"`
	FechaHora time.Time `json:"fecha_hora"`
}

type Solicitud struct {
	IdSolicitud string `json:"id_solicitud"` //idempotencia con IDs
	IdCliente string `json:"id_cliente"`
	Tipo TipoSolicitud `json:"tipo"`
	Mes string `json:"mes,omitempty"`
	IdEvento string `json:"id_evento"`
	IdOcurrencia int `json:"id_ocurrencia"`
	CantidadAsientos int `json:"cantidad_asientos"`
	FechaHora time.Time `json:"fecha_hora"`
}

type Respuesta struct {
	IdSolicitud string `json:"id_solicitud"`
	Exito bool `json:"exito"`
	Mensaje string `json:"mensaje"`
	Eventos []Evento `json:"eventos,omitempty"`
	Reserva *Reserva `json:"reserva,omitempty"`
	AsientosDisponibles int `json:"asientos_disponibles,omitempty"`
	Instancia string `json:"instancia,omitempty"` //instancia del servicio que atendio la solicitud
}

type PeticionDB struct {
	Operacion OperacionDB `json:"operacion"`
	Solicitud Solicitud `json:"solicitud"`
}

func NuevoID(prefijo string) string {
	aleatorio := make([]byte, 8)
	rand.Read(aleatorio)
	return prefijo + "-" + hex.EncodeToString(aleatorio)
}

func (s Solicitud) Validar() error {
	if s.IdSolicitud == "" || s.IdCliente == "" {
		return errors.New("la solicitud llego sin id o sin cliente")
	}
	switch s.Tipo {
	case TipoConsulta:
		if _, err := time.Parse("2006-01", s.Mes); err != nil {
			return fmt.Errorf("el mes %q no sirve, escribalo como 2026-11", s.Mes)
		}
	case TipoReserva:
		if s.IdEvento == "" {
			return errors.New("falta decir cual evento")
		}
		if s.IdOcurrencia < 1 {
			return errors.New("la fecha del evento se cuenta desde 1")
		}
		if s.CantidadAsientos < 1 {
			return errors.New("hay que pedir por lo menos 1 asiento")
		}
	default:
		return fmt.Errorf("no se reconoce la operacion %q", s.Tipo)
	}
	return nil
}

func (s Solicitud) Resumen() string {
	switch s.Tipo {
	case TipoConsulta:
		return fmt.Sprintf("%s consulta los eventos de %s", s.IdCliente, s.Mes)
	case TipoReserva:
		return fmt.Sprintf("%s pide %s de %s en la fecha %d", s.IdCliente, Asientos(s.CantidadAsientos), s.IdEvento, s.IdOcurrencia)
	}
	return fmt.Sprintf("%s manda una solicitud de tipo %q", s.IdCliente, s.Tipo)
}

func Resultado(exito bool) string {
	if exito {
		return "exito"
	}
	return "fallo"
}

func Asientos(n int) string {
	if n == 1 {
		return "1 asiento"
	}
	return fmt.Sprintf("%d asientos", n)
}

func Quedan(n int) string {
	switch n {
	case 0:
		return "no queda ninguno"
	case 1:
		return "solo queda 1"
	}
	return fmt.Sprintf("solo quedan %d", n)
}

func TablaEventos(eventos []Evento) string {
	if len(eventos) == 0 {
		return "no hay eventos para mostrar\n"
	}
	var sb strings.Builder
	capacidad, disponibles := 0, 0
	for _, ev := range eventos {
		fmt.Fprintf(&sb, "%s %s\n", ev.IdEvento, ev.Nombre)
		for _, oc := range ev.Ocurrencias {
			fmt.Fprintf(&sb, "   - %s: %s\n", FechaLegible(oc.Fecha), ocupacion(oc))
			capacidad += oc.CapacidadTotal
			disponibles += oc.AsientosDisponibles
		}
	}
	fmt.Fprintf(&sb, "en total quedan %d de %d asientos libres\n", disponibles, capacidad)
	return sb.String()
}

func ocupacion(oc Ocurrencia) string {
	switch {
	case oc.AsientosDisponibles == 0:
		return fmt.Sprintf("agotado, se vendieron los %d asientos", oc.CapacidadTotal)
	case oc.AsientosVendidos == 0:
		return fmt.Sprintf("%d de %d asientos libres", oc.AsientosDisponibles, oc.CapacidadTotal)
	}
	return fmt.Sprintf("%d de %d asientos libres (%d vendidos)", oc.AsientosDisponibles, oc.CapacidadTotal, oc.AsientosVendidos)
}

func FechaLegible(fecha string) string {
	t, err := time.Parse("2006-01-02", fecha)
	if err != nil {
		return fecha
	}
	meses := []string{"enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"}
	return fmt.Sprintf("%d de %s", t.Day(), meses[t.Month()-1]) // ej: 2026-11-14 queda como 14 de noviembre
}
