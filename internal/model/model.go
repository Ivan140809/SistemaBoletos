// paquete model: los formatos de mensaje que comparten todos los procesos.
//
// como los procesos hablan por la red (zeromq) y pueden estar en maquinas
// distintas, no se comparte nada en memoria: todo viaja como texto json. estos
// structs definen que campos tiene cada mensaje. cada proceso pasa de struct a
// json (json.marshal) para enviar y de json a struct (json.unmarshal) para recibir.
//
// mapa de que viaja por donde:
//
//	cliente  --solicitud-->  gt  --solicitud-->  rmc  --peticiondb-->  persistencia
//	cliente  <--respuesta--  gt  <--respuesta--  rmc  <--respuesta--  persistencia
package model

import (
	"crypto/rand" // numeros aleatorios seguros
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// "type x string" crea un tipo nuevo basado en string. sirve para que el
// compilador distinga, por ejemplo, un tiposolicitud de un string cualquiera.
type TipoSolicitud string

// los dos tipos de solicitud que existen hoy (la modificacion y la cancelacion
// se agregan despues).
const (
	TipoConsulta TipoSolicitud = "consulta"
	TipoReserva  TipoSolicitud = "reserva"
)

// operaciones que rmc le puede pedir a persistencia.
// ojo con la diferencia con tiposolicitud: el tipo es lo que pide el cliente
// (consulta o reserva) y la operacion es lo que rmc le pide a la bd. una reserva
// del cliente se vuelve dos operaciones de bd: consultar_disponibilidad y
// despues crear_reserva.
type OperacionDB string

const (
	OpConsultarEventos        OperacionDB = "consultar_eventos"
	OpConsultarDisponibilidad OperacionDB = "consultar_disponibilidad"
	OpCrearReserva            OperacionDB = "crear_reserva"
)

// estados posibles de una reserva. hoy no se usan en el codigo go (la bd pone
// 'activa' por defecto), quedan listos para cuando se haga la cancelacion.
const (
	ReservaActiva    = "ACTIVA"
	ReservaCancelada = "CANCELADA"
)

// ocurrencia es una fecha concreta de un evento. un evento puede tener 1 o 2
// ocurrencias en el mes y cada una tiene sus propios asientos.
//
// lo que va entre `comillas invertidas` despues de cada campo se llama tag: le
// dice a la libreria json como se llama ese campo en el texto (por ejemplo el
// campo go "idocurrencia" aparece en el json como "id_ocurrencia").
type Ocurrencia struct {
	IdOcurrencia        int    `json:"id_ocurrencia"` // 1 o 2: la primera o la segunda fecha del evento
	Fecha               string `json:"fecha"`         // formato aaaa-mm-dd
	CapacidadTotal      int    `json:"capacidad_total"`
	AsientosDisponibles int    `json:"asientos_disponibles"`
	AsientosVendidos    int    `json:"asientos_vendidos"`
}

// evento es un evento (e1, e2, ...) con su lista de ocurrencias.
// "[]ocurrencia" es una lista (slice) de ocurrencia.
type Evento struct {
	IdEvento    string       `json:"id_evento"`
	Nombre      string       `json:"nombre"`
	Ocurrencias []Ocurrencia `json:"ocurrencias"`
}

// reserva es una reserva que ya esta guardada en la base.
// ojo: se guardan por cantidad de asientos (ej. "5 asientos de e1"), no por
// numero de silla. el sistema no sabe cuales asientos tiene cada cliente.
type Reserva struct {
	IdReserva        string    `json:"id_reserva"` // r0001, r0002, ... (la genera la bd)
	IdCliente        string    `json:"id_cliente"`
	IdOcurrencia     int       `json:"id_ocurrencia"`
	IdEvento         string    `json:"id_evento"`
	CantidadAsientos int       `json:"cantidad_asientos"`
	Estado           string    `json:"estado"`
	Creada           string    `json:"creada"`
	FechaHora        time.Time `json:"fecha_hora"` // hoy no se llena en ningun lado
}

// solicitud es lo que manda el cliente, el mensaje principal del sistema.
// segun el tipo se usan unos campos u otros:
//   - consulta: mes
//   - reserva:  idevento, idocurrencia, cantidadasientos
//
// "omitempty" en el tag quiere decir "si esta vacio, no lo metas en el json".
type Solicitud struct {
	// id unico de cada solicitud (lo genera el cliente con nuevoid). sirve para
	// la idempotencia: si la misma solicitud llega dos veces (por un reintento),
	// la base reconoce el id y no la ejecuta otra vez.
	IdSolicitud      string        `json:"id_solicitud"` //idempotencia con ids
	IdCliente        string        `json:"id_cliente"`   // quien la hace (viene de la opcion -id del cliente)
	Tipo             TipoSolicitud `json:"tipo"`
	Mes              string        `json:"mes,omitempty"` // "2026-11"
	IdEvento         string        `json:"id_evento"`
	IdOcurrencia     int           `json:"id_ocurrencia"`
	CantidadAsientos int           `json:"cantidad_asientos"`
	FechaHora        time.Time     `json:"fecha_hora"` // hoy no se llena en ningun lado
}

// respuesta es lo que se devuelve. la misma estructura sirve para todas las
// operaciones, y segun el caso vienen llenos unos campos u otros.
type Respuesta struct {
	IdSolicitud         string    `json:"id_solicitud"` // para saber a que solicitud responde
	Exito               bool      `json:"exito"`        // true = se pudo, false = se rechazo o fallo
	Mensaje             string    `json:"mensaje"`      // texto que se imprime en el cliente
	Eventos             []Evento  `json:"eventos,omitempty"`
	Reserva             *Reserva  `json:"reserva,omitempty"` // puntero porque puede ser nil si no aplica
	AsientosDisponibles int       `json:"asientos_disponibles,omitempty"`
	Instancia           string    `json:"instancia,omitempty"` //instancia del servicio que atendio la solicitud
}

// peticiondb es lo que rmc le manda a persistencia: la operacion de bd que
// quiere mas la solicitud original del cliente (de ahi saca los datos).
type PeticionDB struct {
	Operacion OperacionDB `json:"operacion"`
	Solicitud Solicitud   `json:"solicitud"`
}

// nuevoid genera un id unico tipo "cliente1-9f8a7c6d5e4b3a21": el prefijo (el
// nombre del cliente) mas 8 bytes aleatorios en hexadecimal.
func NuevoID(prefijo string) string {
	aleatorio := make([]byte, 8) // lista de 8 bytes
	rand.Read(aleatorio)         // la llena con bytes aleatorios
	return prefijo + "-" + hex.EncodeToString(aleatorio)
}

// validar revisa que la solicitud venga bien armada. es un metodo de solicitud
// (el "(s solicitud)" antes del nombre, se llama s.validar()). devuelve nil si
// todo esta bien o un error que dice cual es el problema. el gt la llama antes de
// pasarle algo a rmc.
func (s Solicitud) Validar() error {
	if s.IdSolicitud == "" || s.IdCliente == "" {
		return errors.New("la solicitud llego sin id o sin cliente")
	}
	switch s.Tipo {
	case TipoConsulta:
		// time.parse con "2006-01" revisa que el mes tenga formato aaaa-mm.
		// curiosidad de go: los formatos de fecha no usan "aaaa-mm-dd" sino una
		// fecha de ejemplo fija (2 de enero de 2006). "2006-01-02" es
		// aaaa-mm-dd y "2006-01" es aaaa-mm. asi se escribe el patron.
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

// resumen devuelve una frase corta que describe la solicitud, para los logs.
// ej.: "ana pide 2 asientos de e1 en la fecha 1".
func (s Solicitud) Resumen() string {
	switch s.Tipo {
	case TipoConsulta:
		return fmt.Sprintf("%s consulta los eventos de %s", s.IdCliente, s.Mes)
	case TipoReserva:
		return fmt.Sprintf("%s pide %s de %s en la fecha %d", s.IdCliente, Asientos(s.CantidadAsientos), s.IdEvento, s.IdOcurrencia)
	}
	return fmt.Sprintf("%s manda una solicitud de tipo %q", s.IdCliente, s.Tipo)
}

// resultado pasa true/false a "exito"/"fallo" para los mensajes.
func Resultado(exito bool) string {
	if exito {
		return "exito"
	}
	return "fallo"
}

// asientos arma "1 asiento" o "n asientos" (singular o plural).
func Asientos(n int) string {
	if n == 1 {
		return "1 asiento"
	}
	return fmt.Sprintf("%d asientos", n)
}

// quedan arma "no queda ninguno", "solo queda 1" o "solo quedan n".
func Quedan(n int) string {
	switch n {
	case 0:
		return "no queda ninguno"
	case 1:
		return "solo queda 1"
	}
	return fmt.Sprintf("solo quedan %d", n)
}

// tablaeventos arma el texto que se imprime con la lista de eventos, ej.:
//
//	e1 acm proyectos evento
//	   - 14 de noviembre: 100 de 100 asientos libres
//	en total quedan 100 de 100 asientos libres
//
// la usan el cliente (al consultar) y persistencia (al mostrar la base).
func TablaEventos(eventos []Evento) string {
	if len(eventos) == 0 {
		return "no hay eventos para mostrar\n"
	}
	// strings.builder es la forma eficiente de ir armando un texto largo por pedazos.
	var sb strings.Builder
	capacidad, disponibles := 0, 0 // acumuladores para el total del final
	for _, ev := range eventos {
		// fprintf escribe texto con formato dentro del builder (&sb es su direccion).
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

// ocupacion describe como va una ocurrencia. empieza con minuscula, asi que solo
// se usa dentro de este paquete. "switch {" sin valor evalua cada case como una
// condicion, en orden, y se queda con la primera que sea verdadera.
func ocupacion(oc Ocurrencia) string {
	switch {
	case oc.AsientosDisponibles == 0:
		return fmt.Sprintf("agotado, se vendieron los %d asientos", oc.CapacidadTotal)
	case oc.AsientosVendidos == 0:
		return fmt.Sprintf("%d de %d asientos libres", oc.AsientosDisponibles, oc.CapacidadTotal)
	}
	return fmt.Sprintf("%d de %d asientos libres (%d vendidos)", oc.AsientosDisponibles, oc.CapacidadTotal, oc.AsientosVendidos)
}

// fechalegible pasa "2026-11-14" a "14 de noviembre".
func FechaLegible(fecha string) string {
	t, err := time.Parse("2006-01-02", fecha)
	if err != nil {
		return fecha // si no se entiende, se devuelve tal cual
	}
	meses := []string{"enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"}
	// t.month() va de 1 a 12 pero las listas empiezan en 0, de ahi el "-1".
	return fmt.Sprintf("%d de %s", t.Day(), meses[t.Month()-1]) // ej: 2026-11-14 queda como 14 de noviembre
}
