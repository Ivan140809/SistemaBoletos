// paquete communication, un envoltorio sobre un socket req de zeromq que le agrega un
// tiempo maximo de espera y la recuperacion del socket cuando el otro lado no contesta.
// ESCALABILIDAD HORIZONTAL, si recibe varias direcciones separadas por coma se conecta a todas y reparte las peticiones.
// lo usan el cliente para hablar con el gt y rmc para hablar con persistencia.
// TOLERANCIA A FALLAS, patron lazy pirate completo, si no llega respuesta cierra el socket, abre uno
// nuevo y reenvia la misma peticion hasta Reintentos veces. como la peticion lleva el mismo id de
// solicitud, si la primera si se ejecuto la base devuelve la respuesta guardada y no la repite.
// lo usan el cliente para hablar con el gt y rmc y cancelaciones para hablar con persistencia.
// TOLERANCIA A FALLAS, con NuevoClienteFailover las direcciones no se reparten, se usa una a la vez
// en orden. si la actual no contesta, el reintento sale hacia la siguiente, asi el cliente pasa solo
// del gt primario al gt de respaldo (lado cliente del patron binary star).
package communication

import (
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"strings"
	"time"
)

// ClienteReq guarda todo lo que necesita un cliente req
type ClienteReq struct {
	endpoint string // direccion a donde se conecta
	timeout time.Duration // cuanto espera antes de rendirse
	socket *zmq.Socket // el socket req de verdad
	// TOLERANCIA A FALLAS, modo failover, una direccion a la vez y se pasa a la siguiente si no contesta
	alternar bool
	direcciones []string // las direcciones en orden de preferencia
	actual int // cual direccion se esta usando
	// TOLERANCIA A FALLAS, cuantas veces repetir si no contesta, cero por defecto. con varias direcciones
	// el reintento sale por un socket nuevo y puede caer en otra instancia que si este viva
	Reintentos int
	// TOLERANCIA A FALLAS, si no es nil y devuelve true la respuesta cuenta como fallo y se reintenta,
	// el cliente lo usa para pasar de una vez al otro gt cuando contesta uno pasivo
	Rechazar func(respuesta []byte) bool
	// TOLERANCIA A FALLAS, si no es nil se llama en cada reintento para dejarlo en el log
	AlReintentar func(intento int, err error) // se llama antes de cada reintento, sirve para dejar registro
}

// NuevoClienteReq arma el cliente y lo deja conectado
func NuevoClienteReq(endpoint string, timeout time.Duration) (*ClienteReq, error) {
	// aqui se crea el cliente con su direccion y su tiempo maximo
	c := &ClienteReq{endpoint: endpoint, timeout: timeout}
	if err := c.conectar(); err != nil {
		return nil, err
	}
	return c, nil
}

// NuevoClienteFailover arma un cliente que usa una sola direccion a la vez, en el orden dado,
// y cambia a la siguiente cuando la actual no contesta
func NuevoClienteFailover(endpoint string, timeout time.Duration) (*ClienteReq, error) {
	c := &ClienteReq{endpoint: endpoint, timeout: timeout, alternar: true}
	// separa la lista de direcciones
	for _, ep := range strings.Split(endpoint, ",") {
		if ep = strings.TrimSpace(ep); ep != "" {
			c.direcciones = append(c.direcciones, ep)
		}
	}
	if len(c.direcciones) == 0 {
		return nil, fmt.Errorf("no hay ninguna direccion en %q", endpoint)
	}
	if err := c.conectar(); err != nil {
		return nil, err
	}
	return c, nil
}

// Direccion dice a que direccion esta conectado ahora, sirve para el log del failover
func (c *ClienteReq) Direccion() string {
	if c.alternar {
		return c.direcciones[c.actual]
	}
	return c.endpoint
}

// conectar crea el socket req y lo conecta, se vuelve a usar despues de un timeout
func (c *ClienteReq) conectar() error {
	// aqui se crea el socket req
	socket, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		return err
	}
	socket.SetLinger(0)
	socket.SetRcvtimeo(c.timeout)
	socket.SetSndtimeo(c.timeout)
	socket.SetImmediate(true)
	// TOLERANCIA A FALLAS, en modo failover solo se conecta a la direccion actual
	if c.alternar {
		if err := socket.Connect(c.direcciones[c.actual]); err != nil {
			socket.Close()
			return fmt.Errorf("no se pudo conectar con %s: %w", c.direcciones[c.actual], err)
		}
		c.socket = socket
		return nil
	}
	// ESCALABILIDAD HORIZONTAL, recorre cada direccion separada por coma
	for _, ep := range strings.Split(c.endpoint, ",") {
		ep = strings.TrimSpace(ep)
		if ep == "" {
			continue
		}
		// ESCALABILIDAD HORIZONTAL, se conecta a esta direccion y el req reparte por turnos entre todas
		if err := socket.Connect(ep); err != nil {
			socket.Close()
			return fmt.Errorf("no se pudo conectar con %s: %w", ep, err)
		}
	}
	c.socket = socket
	return nil
}

// Pedir manda una peticion en json y espera la respuesta, reintenta las veces que diga Reintentos.
func (c *ClienteReq) Pedir(peticion, respuesta any) error {
	datos, err := json.Marshal(peticion)
	if err != nil {
		return err
	}
	// TOLERANCIA A FALLAS, el primer intento mas los reinicios, siempre con los mismos datos
	var ultimo error
	for intento := 0; intento <= c.Reintentos; intento++ {
		// avisa que se esta reintentando
		if intento > 0 && c.AlReintentar != nil {
			c.AlReintentar(intento, ultimo)
		}
		recibido, err := c.intentar(datos)
		if err == nil {
			return json.Unmarshal(recibido, respuesta)
		}
		ultimo = err
	}
	// se acabaron los intentos
	if c.Reintentos > 0 {
		return fmt.Errorf("%w, despues de %d intentos", ultimo, c.Reintentos+1)
	}
	return ultimo
}

// intentar hace un solo envio y espera la respuesta hasta el tiempo maximo
func (c *ClienteReq) intentar(datos []byte) ([]byte, error) {
	// se guarda antes de recrear, porque en modo failover recrear cambia de direccion
	direccion := c.Direccion()
	if _, err := c.socket.SendBytes(datos, 0); err != nil {
		// TOLERANCIA A FALLAS, si no se pudo enviar el socket tambien queda inservible
		c.recrear()
		return nil, fmt.Errorf("no se pudo enviar a %s: %w", direccion, err)
	}
	recibido, err := c.socket.RecvBytes(0)
	if err != nil {
		// TOLERANCIA A FALLAS, el req queda trabado esperando respuesta, se cierra y se abre otro.
		// la respuesta vieja, si llega tarde, va al socket cerrado y se pierde, asi no se confunde
		c.recrear()
		return nil, fmt.Errorf("%s no contesto en %v", direccion, c.timeout)
	}
	// TOLERANCIA A FALLAS, una respuesta que no sirve (por ejemplo de un gt pasivo) cuenta como fallo
	if c.Rechazar != nil && c.Rechazar(recibido) {
		c.recrear()
		return nil, fmt.Errorf("%s esta pasivo", direccion)
	}
	return recibido, nil
}

// recrear cierra el socket trabado y abre uno nuevo, patron lazy pirate de zeromq
func (c *ClienteReq) recrear() {
	c.socket.Close()
	// TOLERANCIA A FALLAS, en modo failover el socket nuevo apunta a la siguiente direccion
	if c.alternar {
		c.actual = (c.actual + 1) % len(c.direcciones)
	}
	c.conectar()
}

// Cerrar libera el socket al terminar
func (c *ClienteReq) Cerrar() {
	if c.socket != nil {
		c.socket.Close()
	}
}
