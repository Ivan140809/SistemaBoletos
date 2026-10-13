// Paquete communication: un pequeño "envoltorio" alrededor de un socket REQ de
// ZeroMQ que agrega dos cosas que el socket crudo no trae:
//
//  1. TIMEOUT: si el otro lado no contesta en X tiempo, se rinde con un error
//     en vez de quedarse bloqueado para siempre.
//  2. RECUPERACIÓN: después de un timeout el socket REQ queda inutilizable
//     (ver explicación en Pedir), así que se destruye y se crea uno nuevo.
//
// Lo usan dos procesos: el cliente (para hablar con el GT) y RMC (para hablar
// con persistencia). Está en internal/ porque es código compartido; en Go,
// lo que está dentro de una carpeta "internal" solo puede importarse desde este mismo proyecto.
package communication

import (
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"time"
)

// ClienteReq es un struct: un conjunto de campos agrupados (parecido a una clase
// sin métodos adentro; los métodos se declaran afuera, ver más abajo).
// Los campos que empiezan con minúscula son privados al paquete; los que
// empiezan con mayúscula son públicos. (Lo mismo aplica a funciones: Pedir y
// NuevoClienteReq con mayúscula se pueden usar desde otros paquetes; conectar no.)
type ClienteReq struct {
	endpoint string        // a dónde conectarse, ej. "tcp://192.168.1.10:5555"
	timeout  time.Duration // cuánto esperar antes de rendirse
	socket   *zmq.Socket   // el socket REQ real
}

// NuevoClienteReq es un "constructor" (en Go no existen, es una convención:
// una función que arma y devuelve el struct). Devuelve (*ClienteReq, error):
// el puntero al objeto creado, o el error si no se pudo conectar.
func NuevoClienteReq(endpoint string, timeout time.Duration) (*ClienteReq, error) {
	// "&ClienteReq{...}" crea el struct y devuelve su dirección (puntero).
	c := &ClienteReq{endpoint: endpoint, timeout: timeout}
	if err := c.conectar(); err != nil {
		return nil, err // nil = "nada" (el puntero vacío)
	}
	return c, nil // nil en el error = "no hubo error"
}

// conectar crea el socket REQ y lo conecta. Se separó de NuevoClienteReq
// para poder llamarla de nuevo cuando hay que recrear el socket tras un timeout.
// "(c *ClienteReq)" antes del nombre significa que es un MÉTODO de ClienteReq
// (c es el equivalente a "this"/"self").
func (c *ClienteReq) conectar() error {
	socket, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		return err
	}
	// Linger = cuánto tiempo se queda el socket intentando enviar mensajes
	// pendientes al cerrarse. 0 = descartar de una, no esperar.
	socket.SetLinger(0) // para que no se quede esperando a que se envíe el mensaje
	// Rcvtimeo / Sndtimeo = máximo de tiempo que bloquea un Recv / Send.
	// Pasado ese tiempo, la llamada devuelve error en vez de colgarse.
	socket.SetRcvtimeo(c.timeout) // para que no se quede esperando a que llegue la respuesta
	socket.SetSndtimeo(c.timeout) // para que no se quede bloqueado esperando la repuesta
	if err := socket.Connect(c.endpoint); err != nil {
		socket.Close()
		// %w envuelve el error original dentro de uno nuevo con más contexto.
		return fmt.Errorf("no se pudo conectar con %s: %w", c.endpoint, err)
	}
	c.socket = socket
	return nil
}

// Pedir envía "peticion" (cualquier struct, se convierte a JSON) y espera la
// respuesta, que se escribe en "respuesta" (hay que pasar un puntero a un struct
// vacío). "any" significa "cualquier tipo" (alias de interface{}).
func (c *ClienteReq) Pedir(peticion, respuesta any) error {
	datos, err := json.Marshal(peticion)
	if err != nil {
		return err
	}
	if _, err := c.socket.SendBytes(datos, 0); err != nil {
		return fmt.Errorf("no se pudo enviar a %s: %w", c.endpoint, err)
	}
	// Se bloquea hasta que llegue la respuesta o venza el timeout.
	recibido, err := c.socket.RecvBytes(0)

	if err != nil {
		// IMPORTANTE: un socket REQ funciona como una máquina de estados rígida:
		// después de enviar, queda esperando la respuesta y NO deja enviar otra
		// petición hasta recibirla. Si la respuesta nunca llegó (timeout), el
		// socket se queda trabado en ese estado. La solución estándar (patrón
		// "Lazy Pirate" de la guía de ZeroMQ) es cerrarlo y crear uno nuevo.
		c.socket.Close() // recreacion del REQ
		if errConexion := c.conectar(); errConexion != nil {
			return errConexion
		}
		return fmt.Errorf("%s no contesto en %v", c.endpoint, c.timeout)
	}
	// Convierte el JSON recibido al struct de respuesta.
	return json.Unmarshal(recibido, respuesta)
}

// Cerrar libera el socket. Se llama con "defer" al final de quien lo creó.
func (c *ClienteReq) Cerrar() {
	if c.socket != nil {
		c.socket.Close()
	}
}
