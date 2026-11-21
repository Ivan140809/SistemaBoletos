// paquete communication, un envoltorio sobre un socket req de zeromq que le agrega un
// tiempo maximo de espera y la recuperacion del socket cuando el otro lado no contesta.
// ESCALABILIDAD HORIZONTAL, si recibe varias direcciones separadas por coma se conecta a todas y reparte las peticiones.
// lo usan el cliente para hablar con el gt y rmc para hablar con persistencia.
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

// Pedir manda una peticion en json y espera la respuesta
func (c *ClienteReq) Pedir(peticion, respuesta any) error {
	datos, err := json.Marshal(peticion)
	if err != nil {
		return err
	}
	if _, err := c.socket.SendBytes(datos, 0); err != nil {
		return fmt.Errorf("no se pudo enviar a %s: %w", c.endpoint, err)
	}
	recibido, err := c.socket.RecvBytes(0)

	if err != nil {
		// se cierra el socket trabado, patron lazy pirate de zeromq
		c.socket.Close()
		// aqui se crea un socket nuevo para reemplazar el trabado
		if errConexion := c.conectar(); errConexion != nil {
			return errConexion
		}
		return fmt.Errorf("%s no contesto en %v", c.endpoint, c.timeout)
	}
	return json.Unmarshal(recibido, respuesta)
}

// Cerrar libera el socket al terminar
func (c *ClienteReq) Cerrar() {
	if c.socket != nil {
		c.socket.Close()
	}
}
