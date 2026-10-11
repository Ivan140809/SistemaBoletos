package communication
import (
	"encoding/json"
	"fmt"
	"time"
	zmq "github.com/pebbe/zmq4"
)

type ClienteReq struct {
	endpoint string
	timeout time.Duration
	socket*zmq.Socket
}

func NuevoClienteReq(endpoint string, timeout time.Duration) (*ClienteReq, error) {
 c := &ClienteReq{endpoint: endpoint, timeout: timeout}
	if err := c.conectar(); err != nil {
		return nil, err
	}
	return c, nil
}
func (c *ClienteReq) conectar() error {
  socket, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		return err
	}
	socket.SetLinger(0) // para que no se quede esperando a que se envíe el mensaje
	socket.SetRcvtimeo(c.timeout) // para que no se quede esperando a que llegue la respuesta
	socket.SetSndtimeo(c.timeout) // para que no se quede bloqueado esperando la repuesta
	if err := socket.Connect(c.endpoint); err != nil {
		socket.Close()
		return fmt.Errorf("error al conectar con %s: %w", c.endpoint, err)
	}
	c.socket = socket
	return nil
}

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
		c.socket.Close() // recreacion del REQ
		if errConexion := c.conectar(); errConexion != nil {
			return errConexion
		}
		return fmt.Errorf("sin respuesta de %s después de %v", c.endpoint, c.timeout)
	}
	return json.Unmarshal(recibido, respuesta)
}

func (c *ClienteReq) Cerrar() {
	if c.socket != nil {
		c.socket.Close()
	}
}
