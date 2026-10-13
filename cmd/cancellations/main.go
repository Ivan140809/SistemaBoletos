// servicio de cancelaciones, maquina 2 (capa de negocio).
//
// por ahora es solo un esqueleto para la segunda entrega: se conecta al gt, se
// queda escuchando y cuando llega algo solo imprime "llego una cancelacion".
// no lee el mensaje, no toca la base ni avisa a ningun cliente. ademas el gt
// todavia no le manda nada, porque validar() rechaza todo lo que no sea consulta
// o reserva.
//
// como deberia quedar al final segun el enunciado:
//
//	cliente -> gt (responde "recibida" al toque) -> push/pull -> este servicio
//	-> pide a persistencia liberar los asientos -> cuando persistencia confirma,
//	avisa directo al cliente.
//
// push/pull es un pipeline asincrono: el que hace push deja el mensaje y sigue
// con lo suyo sin esperar respuesta, y el que hace pull los va sacando.
package main

import (
	"flag"
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
)

func main() {
	gt := flag.String("gt", "tcp://localhost:5557", "endpoint PUSH del Gestor de Transacciones")
	flag.Parse()
	log.SetOutput(os.Stdout)
	log.SetPrefix("cancelaciones ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// pull es el lado que recibe en un pipeline. se conecta al push del gt (5557).
	entrada, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		log.Fatal(err)
	}
	defer entrada.Close()
	if err := entrada.Connect(*gt); err != nil {
		log.Fatalf("no se pudo conectar al gestor: %v", err)
	}
	log.Printf("listo, conectado al gestor en %s", *gt)

	// ciclo infinito que recibe y solo avisa que llego algo.
	// el "_" tira el contenido del mensaje porque todavia no lo usamos.
	for {
		if _, err := entrada.RecvBytes(0); err != nil {
			log.Printf("no se pudo recibir: %v", err)
			continue
		}
		log.Printf("llego una cancelacion")
	}
}
