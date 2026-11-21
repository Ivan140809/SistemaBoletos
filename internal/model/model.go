// paquete model, los formatos de mensaje que comparten todos los procesos del sistema.
// como los procesos pueden estar en maquinas distintas todo viaja como json, el cliente manda
// una solicitud al gt, el gt la pasa a rmc, rmc le manda una peticiondb a persistencia
// y la respuesta vuelve por el mismo camino.
package model

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// TipoSolicitud es lo que pide el cliente
type TipoSolicitud string

// los dos tipos de solicitud que existen hoy
const (
	TipoConsulta TipoSolicitud = "consulta" // consultar eventos de un mes
	TipoReserva  TipoSolicitud = "reserva"  // reservar asientos
)

// OperacionDB es lo que rmc le pide a persistencia, una reserva se vuelve dos operaciones
type OperacionDB string

// operaciones que rmc le puede pedir a persistencia
const (
	OpConsultarEventos        OperacionDB = "consultar_eventos"        // eventos de un mes
	OpConsultarDisponibilidad OperacionDB = "consultar_disponibilidad" // asientos libres de una fecha
	OpCrearReserva            OperacionDB = "crear_reserva"            // guardar la reserva
)

// estados de una reserva, quedan listos para cuando se haga la cancelacion
const (
	ReservaActiva    = "ACTIVA"    // reserva vigente
	ReservaCancelada = "CANCELADA" // reserva cancelada
)

// Ocurrencia es una fecha concreta de un evento con sus propios asientos
type Ocurrencia struct {
	IdOcurrencia        int    `json:"id_ocurrencia"`        // primera o segunda fecha del evento
	Fecha               string `json:"fecha"`                // fecha en formato aaaa-mm-dd
	CapacidadTotal      int    `json:"capacidad_total"`      // asientos totales
	AsientosDisponibles int    `json:"asientos_disponibles"` // asientos libres
	AsientosVendidos    int    `json:"asientos_vendidos"`    // asientos vendidos
}

// Evento es un evento con su lista de fechas
type Evento struct {
	IdEvento    string       `json:"id_evento"`   // identificador como E1
	Nombre      string       `json:"nombre"`      // nombre del evento
	Ocurrencias []Ocurrencia `json:"ocurrencias"` // sus fechas
}

// Reserva es una reserva ya guardada, se guarda por cantidad de asientos y no por silla
type Reserva struct {
	IdReserva        string    `json:"id_reserva"`        // la genera la base como R0001
	IdCliente        string    `json:"id_cliente"`        // quien reservo
	IdOcurrencia     int       `json:"id_ocurrencia"`     // fecha del evento
	IdEvento         string    `json:"id_evento"`         // evento reservado
	CantidadAsientos int       `json:"cantidad_asientos"` // cuantos asientos
	Estado           string    `json:"estado"`            // activa o cancelada
	Creada           string    `json:"creada"`            // cuando se creo
	FechaHora        time.Time `json:"fecha_hora"`        // hoy no se llena en ningun lado
}

// Solicitud es lo que manda el cliente, consulta usa mes y reserva usa evento fecha y asientos
type Solicitud struct {
	IdSolicitud      string        `json:"id_solicitud"`      // id unico, la base lo usa para no ejecutar dos veces lo mismo
	IdCliente        string        `json:"id_cliente"`        // quien la hace
	Tipo             TipoSolicitud `json:"tipo"`              // consulta o reserva
	Mes              string        `json:"mes,omitempty"`     // mes a consultar como 2026-11
	IdEvento         string        `json:"id_evento"`         // evento a reservar
	IdOcurrencia     int           `json:"id_ocurrencia"`     // fecha a reservar
	CantidadAsientos int           `json:"cantidad_asientos"` // cuantos asientos
	FechaHora        time.Time     `json:"fecha_hora"`        // hoy no se llena en ningun lado
}

// Respuesta es lo que se devuelve en todas las operaciones
type Respuesta struct {
	IdSolicitud         string   `json:"id_solicitud"`                   // a que solicitud responde
	Exito               bool     `json:"exito"`                          // true si se pudo
	Mensaje             string   `json:"mensaje"`                        // texto que ve el cliente
	Eventos             []Evento `json:"eventos,omitempty"`              // eventos de una consulta
	Reserva             *Reserva `json:"reserva,omitempty"`              // reserva creada, puede venir vacia
	AsientosDisponibles int      `json:"asientos_disponibles,omitempty"` // asientos libres
	Instancia           string   `json:"instancia,omitempty"`            // ESCALABILIDAD HORIZONTAL, cual rmc atendio
}

// PeticionDB es lo que rmc le manda a persistencia, la operacion mas la solicitud original
type PeticionDB struct {
	Operacion OperacionDB `json:"operacion"` // que hacer en la base
	Solicitud Solicitud   `json:"solicitud"` // datos del cliente
}

// NuevoID genera un id unico con el nombre del cliente y 8 bytes aleatorios
func NuevoID(prefijo string) string {
	// aqui se crea la lista de 8 bytes
	aleatorio := make([]byte, 8)
	// la llena con bytes aleatorios
	rand.Read(aleatorio)
	// une el prefijo con los bytes en hexadecimal
	return prefijo + "-" + hex.EncodeToString(aleatorio)
}

// Validar revisa que la solicitud venga bien armada, el gt la llama antes de pasarla a rmc
func (s Solicitud) Validar() error {
	// toda solicitud necesita id y cliente
	if s.IdSolicitud == "" || s.IdCliente == "" {
		return errors.New("la solicitud llego sin id o sin cliente")
	}
	// revisa segun el tipo
	switch s.Tipo {
	case TipoConsulta:
		// el mes debe tener formato aaaa-mm
		if _, err := time.Parse("2006-01", s.Mes); err != nil {
			return fmt.Errorf("el mes %q no sirve, escribalo como 2026-11", s.Mes)
		}
	case TipoReserva:
		// debe decir el evento
		if s.IdEvento == "" {
			return errors.New("falta decir cual evento")
		}
		// la fecha empieza en 1
		if s.IdOcurrencia < 1 {
			return errors.New("la fecha del evento se cuenta desde 1")
		}
		// al menos un asiento
		if s.CantidadAsientos < 1 {
			return errors.New("hay que pedir por lo menos 1 asiento")
		}
	default:
		// tipo desconocido
		return fmt.Errorf("no se reconoce la operacion %q", s.Tipo)
	}
	// todo bien
	return nil
}

// Resumen devuelve una frase corta de la solicitud para los logs
func (s Solicitud) Resumen() string {
	// frase segun el tipo
	switch s.Tipo {
	case TipoConsulta:
		return fmt.Sprintf("%s consulta los eventos de %s", s.IdCliente, s.Mes)
	case TipoReserva:
		return fmt.Sprintf("%s pide %s de %s en la fecha %d", s.IdCliente, Asientos(s.CantidadAsientos), s.IdEvento, s.IdOcurrencia)
	}
	// tipo desconocido
	return fmt.Sprintf("%s manda una solicitud de tipo %q", s.IdCliente, s.Tipo)
}

// Resultado pasa true o false a exito o fallo
func Resultado(exito bool) string {
	if exito {
		return "exito"
	}
	return "fallo"
}

// Asientos arma 1 asiento o n asientos
func Asientos(n int) string {
	if n == 1 {
		return "1 asiento"
	}
	return fmt.Sprintf("%d asientos", n)
}

// Quedan arma la frase de cuantos asientos quedan
func Quedan(n int) string {
	switch n {
	case 0:
		return "no queda ninguno"
	case 1:
		return "solo queda 1"
	}
	return fmt.Sprintf("solo quedan %d", n)
}

// TablaEventos arma el texto con la lista de eventos, la usan el cliente y persistencia
func TablaEventos(eventos []Evento) string {
	// si no hay eventos lo dice
	if len(eventos) == 0 {
		return "no hay eventos para mostrar\n"
	}
	// aqui se crea el texto donde se va armando la tabla
	var sb strings.Builder
	// totales para el final
	capacidad, disponibles := 0, 0
	// recorre cada evento
	for _, ev := range eventos {
		// escribe el id y el nombre del evento
		fmt.Fprintf(&sb, "%s %s\n", ev.IdEvento, ev.Nombre)
		// recorre cada fecha del evento
		for _, oc := range ev.Ocurrencias {
			// escribe la fecha y como va de asientos
			fmt.Fprintf(&sb, "   - %s: %s\n", FechaLegible(oc.Fecha), ocupacion(oc))
			// suma a los totales
			capacidad += oc.CapacidadTotal
			disponibles += oc.AsientosDisponibles
		}
	}
	// escribe el total
	fmt.Fprintf(&sb, "en total quedan %d de %d asientos libres\n", disponibles, capacidad)
	return sb.String()
}

// ocupacion describe como va una fecha, agotada, sin ventas o con ventas
func ocupacion(oc Ocurrencia) string {
	switch {
	case oc.AsientosDisponibles == 0:
		return fmt.Sprintf("agotado, se vendieron los %d asientos", oc.CapacidadTotal)
	case oc.AsientosVendidos == 0:
		return fmt.Sprintf("%d de %d asientos libres", oc.AsientosDisponibles, oc.CapacidadTotal)
	}
	return fmt.Sprintf("%d de %d asientos libres (%d vendidos)", oc.AsientosDisponibles, oc.CapacidadTotal, oc.AsientosVendidos)
}

// FechaLegible pasa 2026-11-14 a 14 de noviembre
func FechaLegible(fecha string) string {
	// convierte el texto a fecha
	t, err := time.Parse("2006-01-02", fecha)
	// si no se entiende la devuelve tal cual
	if err != nil {
		return fecha
	}
	// nombres de los meses
	meses := []string{"enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"}
	// el mes va de 1 a 12 y la lista empieza en 0, por eso se resta 1
	return fmt.Sprintf("%d de %s", t.Day(), meses[t.Month()-1])
}
