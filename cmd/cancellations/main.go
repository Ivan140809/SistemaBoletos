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
		log.Fatalf("no se pudo conectar al gestor: %v", err)
	}
	log.Printf("listo, conectado al gestor en %s", *gt)

	for {
		if _, err := entrada.RecvBytes(0); err != nil {
			log.Printf("no se pudo recibir: %v", err)
			continue
		}
		log.Printf("llego una cancelacion")
	}
}
