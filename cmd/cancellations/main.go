// servicio de cancelaciones de la maquina 2, por ahora es un esqueleto para la segunda entrega.
// se conecta al push del gt con un pull, se queda escuchando y solo avisa cuando llega algo.
// al final debe pedirle a persistencia liberar los asientos y avisarle al cliente sin que este espere.
package main

import (
	"flag"
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
)

// main arranca el servicio y escucha cancelaciones para siempre
func main() {
	// direccion del push del gt
	gt := flag.String("gt", "tcp://localhost:5557", "endpoint PUSH del Gestor de Transacciones")
	// lee todas las opciones que se pasaron
	flag.Parse()
	// los mensajes salen por la terminal
	log.SetOutput(os.Stdout)
	// cada mensaje empieza con cancelaciones
	log.SetPrefix("cancelaciones ")
	// cada mensaje lleva la hora con microsegundos
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// aqui se crea el socket pull que recibe del gt
	entrada, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		log.Fatal(err)
	}
	// cierra el socket al terminar
	defer entrada.Close()
	// se conecta al push del gt en el puerto 5557
	if err := entrada.Connect(*gt); err != nil {
		log.Fatalf("no se pudo conectar al gestor: %v", err)
	}
	// avisa que el servicio ya esta listo
	log.Printf("listo, conectado al gestor en %s", *gt)

	// ciclo infinito
	for {
		// espera un mensaje y por ahora descarta su contenido
		if _, err := entrada.RecvBytes(0); err != nil {
			log.Printf("no se pudo recibir: %v", err)
			continue
		}
		// avisa que llego una cancelacion
		log.Printf("llego una cancelacion")
	}
}
