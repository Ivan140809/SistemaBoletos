// SERVICIO DE CANCELACIONES - Máquina 2, capa de negocio.
//
// ESTADO ACTUAL: es solo un ESQUELETO para la segunda entrega. Se conecta al
// GT y se queda escuchando, pero cuando llega algo únicamente imprime
// "llego una cancelacion". No lee el mensaje, no toca la base de datos y no
// notifica a ningún cliente. Además, el GT todavía no le manda nada: Validar()
// rechaza cualquier solicitud que no sea consulta o reserva.
//
// Cómo debería terminar siendo (según el enunciado):
//
//	cliente -> GT (responde "recibida" de inmediato) -> PUSH/PULL -> este servicio
//	-> pide a persistencia liberar los asientos -> cuando persistencia confirma,
//	notifica DIRECTAMENTE al cliente.
//
// PUSH/PULL es un patrón asíncrono "pipeline": el que hace PUSH deja el mensaje
// y sigue con su vida (no espera respuesta); el que hace PULL los va tomando.
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

	// Socket PULL: el que recibe en un pipeline. Conecta al PUSH del GT (puerto 5557).
	entrada, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		log.Fatal(err)
	}
	defer entrada.Close()
	if err := entrada.Connect(*gt); err != nil {
		log.Fatalf("no se pudo conectar al gestor: %v", err)
	}
	log.Printf("listo, conectado al gestor en %s", *gt)

	// Ciclo infinito: recibe mensajes y solo avisa que llegaron.
	// "if _, err := ...; err != nil" declara y revisa el error en una sola línea;
	// el "_" descarta el contenido del mensaje (todavía no se usa).
	for {
		if _, err := entrada.RecvBytes(0); err != nil {
			log.Printf("no se pudo recibir: %v", err)
			continue
		}
		log.Printf("llego una cancelacion")
	}
}
