// cliente de la maquina 1, programa de terminal que simula usuarios con tres subcomandos.
// consultar pide los eventos de un mes, reservar pide asientos y concurrente lanza varios clientes
// a la vez peleando por el mismo evento. el cliente hace connect al gt y la respuesta vuelve
// por esa misma conexion, asi que la m1 no necesita puertos abiertos.
package main

import (
	"flag"
	"fmt"
	"os"
	"sistemaboletos/internal/communication"
	"sistemaboletos/internal/model"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// opcionesComunes junta las opciones que comparten los tres subcomandos
type opcionesComunes struct {
	gt      *string        // direccion del gestor de transacciones
	id      *string        // nombre del cliente
	timeout *time.Duration // cuanto esperar la respuesta
}

// resultadoConcurrente guarda lo que le paso a cada cliente del subcomando concurrente
type resultadoConcurrente struct {
	cliente  string          // nombre del cliente
	pedidos  int             // cuantos asientos pidio
	resp     model.Respuesta // lo que contesto el sistema
	err      error           // error de comunicacion si hubo
	duracion time.Duration   // cuanto tardo la respuesta
}

// main lee el subcomando y llama a la funcion que toca
func main() {
	// sin subcomando muestra la ayuda
	if len(os.Args) < 2 {
		uso()
	}
	// el subcomando y el resto de opciones
	comando, argumentos := os.Args[1], os.Args[2:]
	// error si algo sale mal
	var err error
	// segun el subcomando llama a su funcion
	switch comando {
	case "consultar":
		err = consultar(argumentos)
	case "reservar":
		err = reservar(argumentos)
	case "concurrente":
		err = concurrente(argumentos)
	default:
		uso()
	}
	// si hubo error lo muestra y sale con codigo 1
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// uso imprime la ayuda y sale con codigo 2 de mal uso
func uso() {
	fmt.Fprintln(os.Stderr, `uso: client <comando> [opciones]

comandos:
  consultar, consulta los eventos de un mes
  reservar, reserva asientos de un evento
  concurrente, lanza varios clientes que reservan al mismo tiempo`)
	os.Exit(2)
}

// opciones define -gt -id y -timeout, -gt usa SRB_GT si no se escribe y si no localhost
func opciones(fs *flag.FlagSet, idPorDefecto string) opcionesComunes {
	// aqui se crean las opciones comunes
	return opcionesComunes{
		gt:      fs.String("gt", entorno("SRB_GT", "tcp://localhost:5555"), "endpoint del Gestor de Transacciones"),
		id:      fs.String("id", idPorDefecto, "identificador del cliente"),
		timeout: fs.Duration("timeout", 5*time.Second, "cuanto esperar la respuesta"),
	}
}

// entorno lee una variable de entorno y si esta vacia usa el valor por defecto
func entorno(variable, valorPorDefecto string) string {
	// si la variable tiene valor se usa
	if valor := os.Getenv(variable); valor != "" {
		return valor
	}
	// si no se usa el valor por defecto
	return valorPorDefecto
}

// enviar manda una solicitud al gt y devuelve la respuesta, cuanto tardo y un error
func enviar(op opcionesComunes, s model.Solicitud) (model.Respuesta, time.Duration, error) {
	// aqui llega la respuesta
	var resp model.Respuesta
	// aqui se crea el socket req propio de este envio
	cliente, err := communication.NuevoClienteReq(*op.gt, *op.timeout)
	if err != nil {
		return resp, 0, err
	}
	// cierra el socket al terminar
	defer cliente.Cerrar()
	// marca la hora de inicio para medir el tiempo de respuesta
	inicio := time.Now()
	// manda la solicitud y espera la respuesta
	err = cliente.Pedir(s, &resp)
	// devuelve la respuesta y cuanto tardo
	return resp, time.Since(inicio), err
}

// consultar es el subcomando que pide los eventos de un mes
func consultar(argumentos []string) error {
	// aqui se crean las opciones de este subcomando
	fs := flag.NewFlagSet("consultar", flag.ExitOnError)
	// opciones comunes
	op := opciones(fs, "Cliente1")
	// mes a consultar
	mes := fs.String("mes", "2026-11", "mes a consultar, por ejemplo 2026-11")
	// lee las opciones
	fs.Parse(argumentos)

	// aqui se crea la solicitud de consulta con un id unico
	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoConsulta, Mes: *mes}
	// la manda al gt
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	// ESCALABILIDAD HORIZONTAL, muestra cual instancia de rmc atendio la solicitud
	atendio := ""
	if resp.Instancia != "" {
		atendio = ", lo atendio " + resp.Instancia
	}
	// imprime el resultado y el tiempo
	fmt.Printf("%s consulto los eventos de %s, %s: %s (%v%s)\n", *op.id, *mes, model.Resultado(resp.Exito), resp.Mensaje, duracion.Round(time.Microsecond), atendio)
	// si salio bien imprime la tabla de eventos
	if resp.Exito {
		fmt.Print(model.TablaEventos(resp.Eventos))
	}
	return nil
}

// reservar es el subcomando que pide n asientos de una fecha de un evento
func reservar(argumentos []string) error {
	// aqui se crean las opciones de este subcomando
	fs := flag.NewFlagSet("reservar", flag.ExitOnError)
	// opciones comunes
	op := opciones(fs, "Cliente1")
	// evento a reservar
	evento := fs.String("evento", "E1", "identificador del evento")
	// fecha del evento
	ocurrencia := fs.Int("ocurrencia", 1, "cual fecha del evento, 1 o 2")
	// cuantos asientos
	asientos := fs.Int("asientos", 1, "cantidad de asientos")
	// lee las opciones
	fs.Parse(argumentos)

	// aqui se crea la solicitud de reserva con un id unico
	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoReserva, IdEvento: *evento, IdOcurrencia: *ocurrencia, CantidadAsientos: *asientos}
	// la manda al gt
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	// imprime el resultado y el tiempo
	fmt.Printf("%s pidio %s de %s en la fecha %d, %s: %s (%v)\n", *op.id, model.Asientos(*asientos), *evento, *ocurrencia, model.Resultado(resp.Exito), resp.Mensaje, duracion.Round(time.Microsecond))
	return nil
}

// concurrente lanza varios clientes que reservan el mismo evento en el mismo instante.
// demuestra que no se venden mas asientos de los que hay aunque todos compitan a la vez
func concurrente(argumentos []string) error {
	// aqui se crean las opciones de este subcomando
	fs := flag.NewFlagSet("concurrente", flag.ExitOnError)
	// opciones comunes
	op := opciones(fs, "Concurrente")
	// evento por el que compiten
	evento := fs.String("evento", "E2", "evento por el que compiten los clientes")
	// fecha del evento
	ocurrencia := fs.Int("ocurrencia", 1, "cual fecha del evento")
	// asientos que pide cada cliente separados por coma
	lista := fs.String("asientos", "30,25,40,20,15", "asientos que pide cada cliente, separados por coma")
	// lee las opciones
	fs.Parse(argumentos)

	// lista de cantidades y total pedido
	var cantidades []int
	total := 0
	// parte el texto por comas
	for _, valor := range strings.Split(*lista, ",") {
		// convierte cada pedazo a numero
		cantidad, err := strconv.Atoi(strings.TrimSpace(valor))
		// debe ser un numero de al menos 1
		if err != nil || cantidad < 1 {
			return fmt.Errorf("la cantidad %q no sirve", valor)
		}
		// la agrega a la lista y al total
		cantidades = append(cantidades, cantidad)
		total += cantidad
	}

	// aqui se crea la lista de resultados, cada cliente escribe en su posicion
	resultados := make([]resultadoConcurrente, len(cantidades))
	// aqui se crea el canal salida que suelta a todos los clientes a la vez
	salida := make(chan struct{})
	// contadores para esperar a que todos se conecten y a que todos terminen
	var conectados, terminados sync.WaitGroup
	// lanza un cliente por cada cantidad
	for i, cantidad := range cantidades {
		// uno mas por conectar y por terminar
		conectados.Add(1)
		terminados.Add(1)
		// aqui se crea la goroutine de un cliente
		go func(i, cantidad int) {
			// al terminar avisa que ya acabo
			defer terminados.Done()
			// aqui se crea el resultado de este cliente
			r := resultadoConcurrente{cliente: fmt.Sprintf("%s-%d", *op.id, i+1), pedidos: cantidad}
			// aqui se crea el socket req propio de este cliente
			cliente, err := communication.NuevoClienteReq(*op.gt, *op.timeout)
			// avisa que ya se conecto
			conectados.Done()
			// si no se pudo conectar guarda el error y termina
			if err != nil {
				r.err = err
				resultados[i] = r
				return
			}
			// cierra el socket al terminar
			defer cliente.Cerrar()
			// espera la senal de salida
			<-salida
			// aqui se crea la solicitud de reserva
			s := model.Solicitud{IdSolicitud: model.NuevoID(r.cliente), IdCliente: r.cliente, Tipo: model.TipoReserva, IdEvento: *evento, IdOcurrencia: *ocurrencia, CantidadAsientos: cantidad}
			// mide cuanto tarda la reserva
			inicio := time.Now()
			r.err = cliente.Pedir(s, &r.resp)
			r.duracion = time.Since(inicio)
			// guarda el resultado
			resultados[i] = r
		}(i, cantidad)
	}
	// espera a que todos abran su socket
	conectados.Wait()
	// margen para que las conexiones tcp terminen de armarse
	time.Sleep(300 * time.Millisecond)
	// avisa cuantos salen y cuanto piden
	fmt.Printf("salen %d clientes al mismo tiempo por %s en la fecha %d, entre todos piden %d asientos\n", len(cantidades), *evento, *ocurrencia, total)
	// cierra el canal y suelta a todos los clientes al mismo tiempo
	close(salida)
	// espera a que todos terminen
	terminados.Wait()

	// ordena del que tardo menos al que tardo mas, parecido al orden de atencion
	sort.Slice(resultados, func(a, b int) bool { return resultados[a].duracion < resultados[b].duracion })
	// asientos vendidos y clientes que consiguieron puesto
	concedidos, aceptados := 0, 0
	// recorre cada resultado
	for _, r := range resultados {
		// si hubo error de comunicacion lo muestra y sigue
		if r.err != nil {
			fmt.Printf("%s pidio %s, %s: %v\n", r.cliente, model.Asientos(r.pedidos), model.Resultado(false), r.err)
			continue
		}
		// si consiguio puesto suma sus asientos
		if r.resp.Exito {
			concedidos += r.pedidos
			aceptados++
		}
		// imprime lo que le paso a este cliente
		fmt.Printf("%s pidio %s, %s: %s (%v)\n", r.cliente, model.Asientos(r.pedidos), model.Resultado(r.resp.Exito), r.resp.Mensaje, r.duracion.Round(time.Microsecond))
	}
	// imprime el resumen final
	fmt.Printf("%s, %s y se vendieron %d de los %d asientos pedidos\n", cuantos(aceptados, "cliente consiguio puesto", "clientes consiguieron puesto"), cuantos(len(resultados)-aceptados, "se quedo sin puesto", "se quedaron sin puesto"), concedidos, total)
	return nil
}

// cuantos arma la frase en singular o plural segun n
func cuantos(n int, uno, varios string) string {
	// singular si es uno
	if n == 1 {
		return "1 " + uno
	}
	// plural en los demas casos
	return fmt.Sprintf("%d %s", n, varios)
}
