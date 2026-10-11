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

	entrada, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		log.Fatal(err)
	}
	defer entrada.Close()
	if err := entrada.Connect(*gt); err != nil {
		log.Fatalf("no se pudo conectar al GT %v", err)
	}
	log.Printf("conectado al GT en %s / la operación de cancelar se implementa en la segunda entrega", *gt)

	for {
		if _, err := entrada.RecvBytes(0); err != nil {
			log.Printf("error recibiendo: %v", err)
			continue
		}
		log.Printf("cancelación recibida")
	}
}
