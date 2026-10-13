// Paquete model: los "formatos de mensaje" compartidos por todos los procesos.
//
// Como los procesos hablan por la red (ZeroMQ) y pueden estar en máquinas
// distintas, nada se comparte en memoria: todo se manda como TEXTO JSON.
// Estos structs definen qué campos tiene cada mensaje. Cada proceso convierte
// struct -> JSON (json.Marshal) para enviar y JSON -> struct (json.Unmarshal)
// para recibir.
//
// Mapa de qué viaja por dónde:
//
//	cliente  --Solicitud-->  GT  --Solicitud-->  RMC  --PeticionDB-->  persistencia
//	cliente  <--Respuesta--  GT  <--Respuesta--  RMC  <--Respuesta--  persistencia
package model

import (
	"crypto/rand" // números aleatorios criptográficamente seguros
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// "type X string" crea un tipo nuevo basado en string. Sirve para que el
// compilador distinga, por ejemplo, un TipoSolicitud de un string cualquiera.
type TipoSolicitud string

// Constantes: los dos tipos de solicitud que existen hoy. (La modificación y
// la cancelación se agregarán después.)
const (
	TipoConsulta TipoSolicitud = "consulta"
	TipoReserva  TipoSolicitud = "reserva"
)

// Operaciones que RMC le puede pedir al servicio de persistencia.
// Nótese la diferencia con TipoSolicitud: Tipo es lo que pide el CLIENTE (consulta/reserva);
// Operación es lo que pide RMC a la BD. Una reserva del cliente se convierte en
// DOS operaciones de BD: consultar_disponibilidad y luego crear_reserva.
type OperacionDB string

const (
	OpConsultarEventos        OperacionDB = "consultar_eventos"
	OpConsultarDisponibilidad OperacionDB = "consultar_disponibilidad"
	OpCrearReserva            OperacionDB = "crear_reserva"
)

// Estados posibles de una reserva (hoy no se usan en código Go; la BD pone
// 'ACTIVA' por defecto. Están listos para cuando se implemente la cancelación).
const (
	ReservaActiva    = "ACTIVA"
	ReservaCancelada = "CANCELADA"
)

// Ocurrencia = una fecha concreta de un evento. Un evento puede tener 1 o 2
// ocurrencias en el mes, y cada ocurrencia tiene sus propios asientos.
//
// Lo que está entre `comillas invertidas` después de cada campo se llama "tag":
// le dice a la librería JSON cómo se llama ese campo en el texto JSON
// (ej.: el campo Go "IdOcurrencia" aparece en el JSON como "id_ocurrencia").
type Ocurrencia struct {
	IdOcurrencia        int    `json:"id_ocurrencia"` // 1 o 2: "la primera/segunda fecha del evento"
	Fecha               string `json:"fecha"`         // formato AAAA-MM-DD
	CapacidadTotal      int    `json:"capacidad_total"`
	AsientosDisponibles int    `json:"asientos_disponibles"`
	AsientosVendidos    int    `json:"asientos_vendidos"`
}

// Evento = un evento (E1, E2, ...) con su lista de ocurrencias.
// "[]Ocurrencia" es una lista (slice) de Ocurrencia.
type Evento struct {
	IdEvento    string       `json:"id_evento"`
	Nombre      string       `json:"nombre"`
	Ocurrencias []Ocurrencia `json:"ocurrencias"`
}

// Reserva = una reserva ya guardada en la base de datos.
// OJO: las reservas se guardan POR CANTIDAD de asientos (ej. "5 asientos de E1"),
// no por número de silla. El sistema no sabe "qué" asientos tiene cada cliente.
type Reserva struct {
	IdReserva        string    `json:"id_reserva"` // R0001, R0002, ... (lo genera la BD)
	IdCliente        string    `json:"id_cliente"`
	IdOcurrencia     int       `json:"id_ocurrencia"`
	IdEvento         string    `json:"id_evento"`
	CantidadAsientos int       `json:"cantidad_asientos"`
	Estado           string    `json:"estado"`
	Creada           string    `json:"creada"`
	FechaHora        time.Time `json:"fecha_hora"` // hoy no se llena en ningún lado
}

// Solicitud = lo que manda el cliente. Es el mensaje principal del sistema.
// Según el Tipo se usan unos campos u otros:
//   - consulta: Mes
//   - reserva:  IdEvento, IdOcurrencia, CantidadAsientos
//
// "omitempty" en el tag significa "si está vacío, no lo incluyas en el JSON".
type Solicitud struct {
	// Identificador ÚNICO de cada solicitud (lo genera el cliente con NuevoID).
	// Sirve para IDEMPOTENCIA: si la misma solicitud llegara dos veces (por un
	// reintento), la base de datos reconoce el id y no la ejecuta otra vez.
	IdSolicitud      string        `json:"id_solicitud"` //idempotencia con IDs
	IdCliente        string        `json:"id_cliente"`   // quién la hace (viene de la opción -id del cliente)
	Tipo             TipoSolicitud `json:"tipo"`
	Mes              string        `json:"mes,omitempty"` // "2026-11"
	IdEvento         string        `json:"id_evento"`
	IdOcurrencia     int           `json:"id_ocurrencia"`
	CantidadAsientos int           `json:"cantidad_asientos"`
	FechaHora        time.Time     `json:"fecha_hora"` // hoy no se llena en ningún lado
}

// Respuesta = lo que se devuelve. La misma estructura sirve para todas las
// operaciones: según el caso vienen llenos unos campos u otros.
type Respuesta struct {
	IdSolicitud         string    `json:"id_solicitud"` // para saber a qué solicitud responde
	Exito               bool      `json:"exito"`        // true = se pudo, false = se rechazó o falló
	Mensaje             string    `json:"mensaje"`      // texto legible que se imprime en el cliente
	Eventos             []Evento  `json:"eventos,omitempty"`
	Reserva             *Reserva  `json:"reserva,omitempty"` // puntero: puede ser nil si no aplica
	AsientosDisponibles int       `json:"asientos_disponibles,omitempty"`
	Instancia           string    `json:"instancia,omitempty"` //instancia del servicio que atendio la solicitud
}

// PeticionDB = lo que RMC le manda a persistencia: la operación de BD que
// quiere + la solicitud original del cliente (de ahí saca los datos).
type PeticionDB struct {
	Operacion OperacionDB `json:"operacion"`
	Solicitud Solicitud   `json:"solicitud"`
}

// NuevoID genera un identificador único como "Cliente1-9f8a7c6d5e4b3a21":
// el prefijo (el nombre del cliente) más 8 bytes aleatorios en hexadecimal.
func NuevoID(prefijo string) string {
	aleatorio := make([]byte, 8) // lista de 8 bytes
	rand.Read(aleatorio)         // la llena con bytes aleatorios
	return prefijo + "-" + hex.EncodeToString(aleatorio)
}

// Validar revisa que la solicitud esté bien formada. Es un MÉTODO de Solicitud
// (el "(s Solicitud)" antes del nombre: se llama s.Validar()). Devuelve nil si
// todo está bien, o un error describiendo el problema. El GT lo llama antes de
// pasarle nada a RMC.
func (s Solicitud) Validar() error {
	if s.IdSolicitud == "" || s.IdCliente == "" {
		return errors.New("la solicitud llego sin id o sin cliente")
	}
	switch s.Tipo {
	case TipoConsulta:
		// time.Parse con "2006-01" revisa que el mes tenga el formato AAAA-MM.
		// Curiosidad de Go: los formatos de fecha no usan "AAAA-MM-DD" sino una
		// fecha de ejemplo fija (2 de enero de 2006): "2006-01-02" significa
		// año-mes-día, "2006-01" significa año-mes. Así se escribe el patrón.
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

// Resumen devuelve una frase corta que describe la solicitud, para los logs.
// Ej.: "Ana pide 2 asientos de E1 en la fecha 1".
func (s Solicitud) Resumen() string {
	switch s.Tipo {
	case TipoConsulta:
		return fmt.Sprintf("%s consulta los eventos de %s", s.IdCliente, s.Mes)
	case TipoReserva:
		return fmt.Sprintf("%s pide %s de %s en la fecha %d", s.IdCliente, Asientos(s.CantidadAsientos), s.IdEvento, s.IdOcurrencia)
	}
	return fmt.Sprintf("%s manda una solicitud de tipo %q", s.IdCliente, s.Tipo)
}

// Resultado convierte true/false en "exito"/"fallo" para los mensajes.
func Resultado(exito bool) string {
	if exito {
		return "exito"
	}
	return "fallo"
}

// Asientos arma "1 asiento" o "N asientos" (singular/plural).
func Asientos(n int) string {
	if n == 1 {
		return "1 asiento"
	}
	return fmt.Sprintf("%d asientos", n)
}

// Quedan arma "no queda ninguno", "solo queda 1" o "solo quedan N".
func Quedan(n int) string {
	switch n {
	case 0:
		return "no queda ninguno"
	case 1:
		return "solo queda 1"
	}
	return fmt.Sprintf("solo quedan %d", n)
}

// TablaEventos arma el texto que se imprime con la lista de eventos, ej.:
//
//	E1 ACM proyectos evento
//	   - 14 de noviembre: 100 de 100 asientos libres
//	en total quedan 100 de 100 asientos libres
//
// La usan el cliente (al consultar) y persistencia (al mostrar la base).
func TablaEventos(eventos []Evento) string {
	if len(eventos) == 0 {
		return "no hay eventos para mostrar\n"
	}
	// strings.Builder: forma eficiente de ir armando un texto largo por pedazos.
	var sb strings.Builder
	capacidad, disponibles := 0, 0 // acumuladores para el total final
	for _, ev := range eventos {
		// Fprintf escribe texto con formato dentro del Builder (&sb = su dirección).
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

// ocupacion describe el estado de una ocurrencia. Empieza con minúscula, así que
// solo se usa dentro de este paquete. "switch {" sin valor evalúa cada case
// como una condición, en orden, y toma la primera verdadera.
func ocupacion(oc Ocurrencia) string {
	switch {
	case oc.AsientosDisponibles == 0:
		return fmt.Sprintf("agotado, se vendieron los %d asientos", oc.CapacidadTotal)
	case oc.AsientosVendidos == 0:
		return fmt.Sprintf("%d de %d asientos libres", oc.AsientosDisponibles, oc.CapacidadTotal)
	}
	return fmt.Sprintf("%d de %d asientos libres (%d vendidos)", oc.AsientosDisponibles, oc.CapacidadTotal, oc.AsientosVendidos)
}

// FechaLegible convierte "2026-11-14" en "14 de noviembre".
func FechaLegible(fecha string) string {
	t, err := time.Parse("2006-01-02", fecha)
	if err != nil {
		return fecha // si no se entiende, se devuelve tal cual
	}
	meses := []string{"enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"}
	// t.Month() va de 1 a 12, pero las listas empiezan en 0, por eso el "-1".
	return fmt.Sprintf("%d de %s", t.Day(), meses[t.Month()-1]) // ej: 2026-11-14 queda como 14 de noviembre
}
