// paquete communication, un envoltorio sobre un socket req de zeromq que le agrega un
// tiempo maximo de espera y la recuperacion del socket cuando el otro lado no contesta.
// ESCALABILIDAD HORIZONTAL, si recibe varias direcciones separadas por coma se conecta a todas y reparte las peticiones.
// lo usan el cliente para hablar con el gt y rmc para hablar con persistencia.
// TOLERANCIA A FALLAS, patron lazy pirate completo, si no llega respuesta cierra el socket, abre uno
// nuevo y reenvia la misma peticion hasta Reintentos veces. como la peticion lleva el mismo id de
// solicitud, si la primera si se ejecuto la base devuelve la respuesta guardada y no la repite.
// lo usan el cliente para hablar con el gt y rmc y cancelaciones para hablar con persistencia.
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
	// TOLERANCIA A FALLAS, cuantas veces repetir si no contesta, cero por defecto. con varias direcciones
	// el reintento sale por un socket nuevo y puede caer en otra instancia que si este viva
	Reintentos int
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
	if _, err := c.socket.SendBytes(datos, 0); err != nil {
		// TOLERANCIA A FALLAS, si no se pudo enviar el socket tambien queda inservible
		c.recrear()
		return nil, fmt.Errorf("no se pudo enviar a %s: %w", c.endpoint, err)
	}
	recibido, err := c.socket.RecvBytes(0)
	if err != nil {
		// TOLERANCIA A FALLAS, el req queda trabado esperando respuesta, se cierra y se abre otro.
		// la respuesta vieja, si llega tarde, va al socket cerrado y se pierde, asi no se confunde
		c.recrear()
		return nil, fmt.Errorf("%s no contesto en %v", c.endpoint, c.timeout)
	}
	return recibido, nil
}

// recrear cierra el socket trabado y abre uno nuevo, patron lazy pirate de zeromq
func (c *ClienteReq) recrear() {
	c.socket.Close()
	c.conectar()
}

// Cerrar libera el socket al terminar
func (c *ClienteReq) Cerrar() {
	if c.socket != nil {
		c.socket.Close()
	}
}
