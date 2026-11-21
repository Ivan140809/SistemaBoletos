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
	// lo conecta y si falla devuelve el error
	if err := c.conectar(); err != nil {
		return nil, err
	}
	// devuelve el cliente listo sin error
	return c, nil
}

// conectar crea el socket req y lo conecta, se vuelve a usar despues de un timeout
func (c *ClienteReq) conectar() error {
	// aqui se crea el socket req
	socket, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		return err
	}
	// al cerrar descarta lo pendiente sin esperar
	socket.SetLinger(0)
	// tiempo maximo esperando una respuesta
	socket.SetRcvtimeo(c.timeout)
	// tiempo maximo intentando enviar
	socket.SetSndtimeo(c.timeout)
	// solo manda mensajes a destinos ya conectados para no perderlos en uno caido
	socket.SetImmediate(true)
	// ESCALABILIDAD HORIZONTAL, recorre cada direccion separada por coma
	for _, ep := range strings.Split(c.endpoint, ",") {
		// quita los espacios de la direccion
		ep = strings.TrimSpace(ep)
		// si quedo vacia se salta
		if ep == "" {
			continue
		}
		// ESCALABILIDAD HORIZONTAL, se conecta a esta direccion y el req reparte por turnos entre todas
		if err := socket.Connect(ep); err != nil {
			// si no conecta cierra el socket y avisa
			socket.Close()
			return fmt.Errorf("no se pudo conectar con %s: %w", ep, err)
		}
	}
	// guarda el socket en el cliente
	c.socket = socket
	return nil
}

// Pedir manda una peticion en json y espera la respuesta
func (c *ClienteReq) Pedir(peticion, respuesta any) error {
	// convierte la peticion a json
	datos, err := json.Marshal(peticion)
	if err != nil {
		return err
	}
	// envia la peticion
	if _, err := c.socket.SendBytes(datos, 0); err != nil {
		return fmt.Errorf("no se pudo enviar a %s: %w", c.endpoint, err)
	}
	// espera la respuesta hasta que llegue o se acabe el tiempo
	recibido, err := c.socket.RecvBytes(0)

	// si no llego respuesta el socket queda trabado
	if err != nil {
		// se cierra el socket trabado, patron lazy pirate de zeromq
		c.socket.Close()
		// aqui se crea un socket nuevo para reemplazar el trabado
		if errConexion := c.conectar(); errConexion != nil {
			return errConexion
		}
		// avisa que el otro lado no contesto a tiempo
		return fmt.Errorf("%s no contesto en %v", c.endpoint, c.timeout)
	}
	// pasa el json recibido a la respuesta
	return json.Unmarshal(recibido, respuesta)
}

// Cerrar libera el socket al terminar
func (c *ClienteReq) Cerrar() {
	// solo lo cierra si existe
	if c.socket != nil {
		c.socket.Close()
	}
}
