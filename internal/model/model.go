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
TipoModificacion TipoSolicitud = "modificacion"
TipoCancelacion TipoSolicitud = "cancelacion"
)
type OperacionDB string

const (
OpConsultarEventos OperacionDB = "consultar_eventos"
OpConsultarDisponibilidad OperacionDB = "consultar_disponibilidad"
OpCrearReserva OperacionDB = "crear_reserva"
OpCancelarReserva OperacionDB = "cancelar_reserva"
)
const (
ReservaActiva = "ACTIVA" 
ReservaCancelada = "CANCELADA"
)

type Ocurrencia struct {
IdOcurrencia int    `json:"id_ocurrencia"`
Fecha string `json:"fecha"` 
CapacidadTotal int  `json:"capacidad_total"`
AsientosDisponibles int `json:"asientos_disponibles"`
AsientosVendidos int `json:"asientos_vendidos"`
}
type Evento struct {
IdEvento string `json:"id_evento"`
Nombre string `json:"nombre"`
Ocurrencias []Ocurrencia `json:"ocurrencias"` 
}


type Reserva struct {
IdReserva string`json:"id_reserva"`
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
	IdReserva string `json:"id_reserva,omitempty"`
	FechaHora time.Time `json:"fecha_hora"`
	DireccionNotificacion string `json:"direccion_notificacion,omitempty"` //endpoint donde el cliente espera la notificacion
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
	Solicitud Solicitud   `json:"solicitud"`
}

func NuevoID(prefijo string) string {
	aleatorio := make([]byte, 8)
	rand.Read(aleatorio)
	return prefijo + "-" + hex.EncodeToString(aleatorio)
}

func (s Solicitud) Validar() error {
	if s.IdSolicitud == "" || s.IdCliente == "" {
		return errors.New("faltan idSolicitud o idCliente")
	}
	switch s.Tipo {
	case TipoConsulta:
		if _, err := time.Parse("2006-01", s.Mes); err != nil {
			return fmt.Errorf("mes %q inválido,usa AAAA-MM", s.Mes)
		}
	case TipoReserva, TipoModificacion:
		if s.Tipo == TipoModificacion && s.IdReserva == "" {
			return errors.New("falta idReserva")
		}
		if s.IdEvento == "" {
			return errors.New("falta idEvento")
		}
		if s.IdOcurrencia < 1 {
			return errors.New("idOcurrencia inválido debe ser 1 o mayor")
		}
		if s.CantidadAsientos < 1 {
			return errors.New("la cantidad de asientos inválido debe ser 1 o mayor")
		}
	case TipoCancelacion:
		if s.IdReserva == "" {
			return errors.New("falta idReserva")
		}
		if s.DireccionNotificacion == "" {
			return errors.New("falta la direccion de Notificacion de cliente")
		}
	default:
		return fmt.Errorf("tipo %q inválido", s.Tipo)
	}
	return nil
}

func (s Solicitud) Resumen() string {
	switch s.Tipo {
	case TipoConsulta:
		return fmt.Sprintf("%s cliente=%s mes=%s", s.Tipo, s.IdCliente, s.Mes)
	case TipoReserva, TipoModificacion:
		return fmt.Sprintf("%s cliente=%s evento=%s ocurrencia=%d asientos=%d", s.Tipo, s.IdCliente, s.IdEvento, s.IdOcurrencia, s.CantidadAsientos)
	case TipoCancelacion:
		return fmt.Sprintf("%s cliente=%s reserva=%s", s.Tipo, s.IdCliente, s.IdReserva)
	}
	return fmt.Sprintf("%s cliente=%s", s.Tipo, s.IdCliente)
}

func Resultado(exito bool) string {
	if exito {
		return "exito"
	}
	return "fallo"
}


func TablaEventos(eventos []Evento) string {
	var sb strings.Builder
	capacidad, disponibles, vendidos := 0, 0, 0
	fmt.Fprintf(&sb, "%-7s %-30s %-3s %-11s %9s %11s %9s\n", "Evento", "Nombre", "Oc", "Fecha", "Capacidad", "Disponibles", "Vendidos")
	for _, ev := range eventos {
		for _, oc := range ev.Ocurrencias {
			fmt.Fprintf(&sb, "%-7s %-30s %-3d %-11s %9d %11d %9d\n", ev.IdEvento, recortar(ev.Nombre, 30), oc.IdOcurrencia, oc.Fecha, oc.CapacidadTotal, oc.AsientosDisponibles, oc.AsientosVendidos)
			capacidad += oc.CapacidadTotal
			disponibles += oc.AsientosDisponibles
			vendidos += oc.AsientosVendidos
		}
	}
	fmt.Fprintf(&sb, "%-54s %9d %11d %9d\n", "Total", capacidad, disponibles, vendidos)
	return sb.String()
}


func recortar(texto string, maximo int) string {
	runas := []rune(texto)
	if len(runas) <= maximo {
		return texto
	}
	return string(runas[:maximo-1])
}
