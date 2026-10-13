// paquete communication: un envoltorio chiquito sobre un socket req de zeromq
// que le agrega dos cosas que el socket crudo no trae:
//
//  1. timeout: si el otro lado no contesta en x tiempo, se rinde con un error en
//     vez de quedarse bloqueado para siempre.
//  2. recuperacion: despues de un timeout el socket req queda inservible (la
//     explicacion esta en pedir), asi que se destruye y se crea uno nuevo.
//
// lo usan dos procesos: el cliente (para hablar con el gt) y rmc (para hablar
// con persistencia). esta en internal/ porque es codigo compartido, y en go lo
// que esta dentro de una carpeta "internal" solo se puede importar desde este mismo proyecto.
package communication

import (
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"time"
)

// clientereq es un struct: un grupo de campos juntos (parecido a una clase sin
// metodos adentro, los metodos se declaran afuera mas abajo).
// los campos con minuscula son privados al paquete y los de mayuscula son
// publicos. con las funciones pasa igual: pedir y nuevoclientereq (con mayuscula)
// se pueden usar desde otros paquetes, conectar no.
type ClienteReq struct {
	endpoint string        // a donde conectarse, ej. "tcp://192.168.1.10:5555"
	timeout  time.Duration // cuanto esperar antes de rendirse
	socket   *zmq.Socket   // el socket req de verdad
}

// nuevoclientereq es un "constructor" (en go no existen, es solo una convencion:
// una funcion que arma y devuelve el struct). devuelve (*clientereq, error):
// el puntero al objeto creado, o el error si no pudo conectar.
func NuevoClienteReq(endpoint string, timeout time.Duration) (*ClienteReq, error) {
	// "&clientereq{...}" crea el struct y devuelve su direccion (un puntero).
	c := &ClienteReq{endpoint: endpoint, timeout: timeout}
	if err := c.conectar(); err != nil {
		return nil, err // nil aqui es "nada" (el puntero vacio)
	}
	return c, nil // nil en el error es "no hubo error"
}

// conectar crea el socket req y lo conecta. esta separada de nuevoclientereq para
// poder llamarla otra vez cuando toca recrear el socket despues de un timeout.
// "(c *clientereq)" antes del nombre quiere decir que es un metodo de
// clientereq (c viene siendo el "this" o "self").
func (c *ClienteReq) conectar() error {
	socket, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		return err
	}
	// linger es cuanto tiempo se queda el socket tratando de mandar lo pendiente
	// al cerrarse. 0 es descartar de una, sin esperar.
	socket.SetLinger(0) // asi no se queda esperando a que salga el mensaje
	// rcvtimeo y sndtimeo son el maximo que bloquea un recv o un send. pasado ese
	// tiempo la llamada devuelve error en vez de colgarse.
	socket.SetRcvtimeo(c.timeout) // asi no se queda esperando una respuesta que no llega
	socket.SetSndtimeo(c.timeout) // y tampoco se queda pegado al enviar
	if err := socket.Connect(c.endpoint); err != nil {
		socket.Close()
		// %w mete el error original dentro de uno nuevo con mas contexto.
		return fmt.Errorf("no se pudo conectar con %s: %w", c.endpoint, err)
	}
	c.socket = socket
	return nil
}

// pedir manda "peticion" (cualquier struct, se pasa a json) y espera la
// respuesta, que se escribe en "respuesta" (hay que pasar un puntero a un struct
// vacio). "any" es "cualquier tipo" (alias de interface{}).
func (c *ClienteReq) Pedir(peticion, respuesta any) error {
	datos, err := json.Marshal(peticion)
	if err != nil {
		return err
	}
	if _, err := c.socket.SendBytes(datos, 0); err != nil {
		return fmt.Errorf("no se pudo enviar a %s: %w", c.endpoint, err)
	}
	// se bloquea hasta que llegue la respuesta o venza el timeout.
	recibido, err := c.socket.RecvBytes(0)

	if err != nil {
		// importante: un socket req es una maquina de estados bien rigida. despues
		// de enviar queda esperando la respuesta y no deja enviar otra peticion
		// hasta recibirla. si la respuesta nunca llego (timeout), el socket se queda
		// trabado ahi. la solucion de siempre (patron "lazy pirate" de la guia de
		// zeromq) es cerrarlo y crear uno nuevo.
		c.socket.Close() // recreacion del req
		if errConexion := c.conectar(); errConexion != nil {
			return errConexion
		}
		return fmt.Errorf("%s no contesto en %v", c.endpoint, c.timeout)
	}
	// pasa el json recibido al struct de respuesta.
	return json.Unmarshal(recibido, respuesta)
}

// cerrar libera el socket. se llama con "defer" al final de quien lo creo.
func (c *ClienteReq) Cerrar() {
	if c.socket != nil {
		c.socket.Close()
	}
}
