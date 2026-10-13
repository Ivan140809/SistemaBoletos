// cliente, maquina 1 (capa de consumidores).
//
// programa de linea de comandos que simula usuarios. tiene tres subcomandos:
//
//	client consultar     -> pide al gt los eventos de un mes
//	client reservar      -> pide al gt reservar asientos de un evento
//	client concurrente   -> lanza varios clientes a la vez peleando por el mismo evento
//
// el cliente es quien abre la conexion hacia el gt (connect, al puerto 5555 de
// la m2) y la respuesta vuelve por esa misma conexion. por eso la m1 no necesita
// puertos abiertos ni que nadie se conecte a ella (por ahora: las notificaciones
// de cancelacion de la segunda entrega si van a obligar a pensar como contactar
// al cliente).
//
// cada ejecucion manda una sola solicitud (o varias en paralelo en "concurrente")
// y termina. todavia no es un generador de carga continua.
package main

import (
	"flag"
	"fmt"
	"os"
	"sistemaboletos/internal/communication"
	"sistemaboletos/internal/model"
	"sort"
	"strconv" // pasar texto a numero (atoi)
	"strings"
	"sync" // para coordinar goroutines (waitgroup)
	"time"
)

// opcionescomunes junta las opciones que comparten los tres subcomandos.
// son punteros porque flag devuelve punteros (se llenan al hacer parse).
type opcionesComunes struct {
	gt      *string        // direccion del gestor de transacciones
	id      *string        // nombre del cliente
	timeout *time.Duration // cuanto esperar la respuesta
}

// resultadoconcurrente guarda lo que le paso a cada cliente del subcomando concurrente.
type resultadoConcurrente struct {
	cliente  string
	pedidos  int // cuantos asientos pidio
	resp     model.Respuesta
	err      error // error de comunicacion, si hubo (timeout, etc.)
	duracion time.Duration
}

func main() {
	// os.args es la lista de palabras escritas en la terminal:
	// os.args[0] es el nombre del programa, os.args[1] el subcomando y el resto las opciones.
	if len(os.Args) < 2 {
		uso()
	}
	comando, argumentos := os.Args[1], os.Args[2:]
	var err error
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
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1) // codigo de salida 1 = termino con error
	}
}

// uso imprime la ayuda y termina el programa (codigo 2 = mal uso).
func uso() {
	fmt.Fprintln(os.Stderr, `uso: client <comando> [opciones]

comandos:
  consultar, consulta los eventos de un mes
  reservar, reserva asientos de un evento
  concurrente, lanza varios clientes que reservan al mismo tiempo`)
	os.Exit(2)
}

// opciones define las tres opciones comunes sobre el conjunto de opciones "fs"
// de un subcomando:
//
//	-gt       donde esta el gt, con formato tcp://ip:puerto (ej. tcp://192.168.1.20:5555).
//	          si no se escribe, usa la variable de entorno srb_gt, y si tampoco existe,
//	          tcp://localhost:5555. prioridad: opcion > variable > valor por defecto.
//	-id       nombre de este cliente (queda guardado en la reserva y en los logs).
//	-timeout  cuanto esperar la respuesta antes de rendirse (ej. 5s, 500ms).
func opciones(fs *flag.FlagSet, idPorDefecto string) opcionesComunes {
	return opcionesComunes{
		gt:      fs.String("gt", entorno("SRB_GT", "tcp://localhost:5555"), "endpoint del Gestor de Transacciones"),
		id:      fs.String("id", idPorDefecto, "identificador del cliente"),
		timeout: fs.Duration("timeout", 5*time.Second, "cuanto esperar la respuesta"),
	}
}

// entorno lee una variable de entorno y si no existe o esta vacia usa el valor por defecto.
func entorno(variable, valorPorDefecto string) string {
	if valor := os.Getenv(variable); valor != "" {
		return valor
	}
	return valorPorDefecto
}

// enviar manda una solicitud al gt y devuelve la respuesta, cuanto tardo y un error.
// en go una funcion puede devolver varios valores a la vez.
func enviar(op opcionesComunes, s model.Solicitud) (model.Respuesta, time.Duration, error) {
	var resp model.Respuesta
	// cada envio crea su propio socket req conectado al gt (op.gt es un puntero, de ahi el *).
	cliente, err := communication.NuevoClienteReq(*op.gt, *op.timeout)
	if err != nil {
		return resp, 0, err
	}
	defer cliente.Cerrar()
	// el tiempo de respuesta (ida y vuelta completa) es la diferencia entre antes
	// y despues de pedir. de aqui salen las metricas de rendimiento.
	inicio := time.Now()
	err = cliente.Pedir(s, &resp)
	return resp, time.Since(inicio), err
}

// consultar es el subcomando "client consultar", pide los eventos de un mes.
func consultar(argumentos []string) error {
	// cada subcomando tiene su propio conjunto de opciones (flagset).
	// exitonerror: si las opciones estan mal escritas, imprime la ayuda y termina.
	fs := flag.NewFlagSet("consultar", flag.ExitOnError)
	op := opciones(fs, "Cliente1")
	mes := fs.String("mes", "2026-11", "mes a consultar, por ejemplo 2026-11")
	fs.Parse(argumentos)

	// se arma la solicitud. nuevoid genera un id unico tipo "cliente1-a1b2c3...".
	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoConsulta, Mes: *mes}
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	// "lo atendio rmc-1": la respuesta trae el nombre de la instancia que la atendio.
	atendio := ""
	if resp.Instancia != "" {
		atendio = ", lo atendio " + resp.Instancia
	}
	fmt.Printf("%s consulto los eventos de %s, %s: %s (%v%s)\n", *op.id, *mes, model.Resultado(resp.Exito), resp.Mensaje, duracion.Round(time.Microsecond), atendio)
	if resp.Exito {
		fmt.Print(model.TablaEventos(resp.Eventos))
	}
	return nil
}

// reservar es el subcomando "client reservar", pide n asientos de una fecha de un evento.
// las reservas son por cantidad de asientos, no se elige el numero de silla.
func reservar(argumentos []string) error {
	fs := flag.NewFlagSet("reservar", flag.ExitOnError)
	op := opciones(fs, "Cliente1")
	evento := fs.String("evento", "E1", "identificador del evento")
	ocurrencia := fs.Int("ocurrencia", 1, "cual fecha del evento, 1 o 2")
	asientos := fs.Int("asientos", 1, "cantidad de asientos")
	fs.Parse(argumentos)

	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoReserva, IdEvento: *evento, IdOcurrencia: *ocurrencia, CantidadAsientos: *asientos}
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	fmt.Printf("%s pidio %s de %s en la fecha %d, %s: %s (%v)\n", *op.id, model.Asientos(*asientos), *evento, *ocurrencia, model.Resultado(resp.Exito), resp.Mensaje, duracion.Round(time.Microsecond))
	return nil
}

// concurrente es el subcomando "client concurrente": simula varios clientes que
// piden reservar el mismo evento exactamente al mismo tiempo.
//
// sirve para demostrar que el sistema no vende mas asientos de los que hay,
// aunque muchos clientes compitan. con -asientos 30,25,40,20,15 se lanzan 5
// clientes que piden 30, 25, 40, 20 y 15 asientos de un evento de 100. en total
// piden 130, asi que a alguno le tiene que tocar quedarse sin puesto.
//
// ojo: esto no es el generador de carga del enunciado (20 solicitudes por minuto,
// parametrizable, durante 2-3 minutos, con consultas y reservas mezcladas). es
// solo una demo de competencia por asientos: cada cliente hace una sola
// solicitud y siempre de reserva.
//
// como funciona (la tecnica de la "pistola de salida"):
//  1. se crea una goroutine por cliente. una goroutine es un hilo ligero de go
//     (se lanza con "go") y todas corren a la vez.
//  2. cada una abre su propio socket req (un socket zeromq no se puede compartir
//     entre hilos y un req solo aguanta una peticion a la vez).
//  3. cada una avisa "ya me conecte" y se queda esperando en el canal "salida".
//  4. el hilo principal espera a que todos esten conectados, aguanta 300 ms para
//     que las conexiones tcp terminen de armarse y cierra el canal (close(salida)),
//     lo que despierta a todas las goroutines al mismo instante.
//  5. cada una manda su reserva y mide su tiempo. el hilo principal espera a que
//     terminen todas, ordena los resultados por duracion (mas o menos el orden en
//     que los atendieron) y muestra quien consiguio asientos y cuantos se vendieron.
func concurrente(argumentos []string) error {
	fs := flag.NewFlagSet("concurrente", flag.ExitOnError)
	op := opciones(fs, "Concurrente")
	evento := fs.String("evento", "E2", "evento por el que compiten los clientes")
	ocurrencia := fs.Int("ocurrencia", 1, "cual fecha del evento")
	lista := fs.String("asientos", "30,25,40,20,15", "asientos que pide cada cliente, separados por coma")
	fs.Parse(argumentos)

	// pasa el texto "30,25,40" a la lista de numeros [30, 25, 40].
	// strings.split parte el texto por las comas y atoi ("ascii to integer")
	// convierte cada pedazo.
	var cantidades []int
	total := 0
	for _, valor := range strings.Split(*lista, ",") {
		cantidad, err := strconv.Atoi(strings.TrimSpace(valor))
		if err != nil || cantidad < 1 {
			return fmt.Errorf("la cantidad %q no sirve", valor)
		}
		cantidades = append(cantidades, cantidad)
		total += cantidad
	}

	// lista donde cada goroutine escribe su resultado en su propia posicion (asi
	// no se pisan entre ellas).
	resultados := make([]resultadoConcurrente, len(cantidades))
	// un canal (chan) es un tubo para comunicar goroutines. este es de tipo
	// struct{}, o sea que no lleva datos, solo se usa de senal de "ya".
	salida := make(chan struct{})
	// waitgroup es un contador para esperar a un grupo de goroutines:
	// add(1) suma una, done() resta una y wait() bloquea hasta que llegue a cero.
	// "conectados" espera a que todas abran su socket y "terminados" a que acaben.
	var conectados, terminados sync.WaitGroup
	for i, cantidad := range cantidades {
		conectados.Add(1)
		terminados.Add(1)
		// "go func(...) {...}(i, cantidad)" lanza la funcion anonima en una
		// goroutine, pasandole los valores actuales de i y cantidad.
		go func(i, cantidad int) {
			defer terminados.Done()
			r := resultadoConcurrente{cliente: fmt.Sprintf("%s-%d", *op.id, i+1), pedidos: cantidad}
			cliente, err := communication.NuevoClienteReq(*op.gt, *op.timeout) // cada cliente tiene su propio socket req
			conectados.Done()
			if err != nil {
				r.err = err
				resultados[i] = r
				return
			}
			defer cliente.Cerrar()
			// "<-salida" se queda esperando hasta que cierren el canal (la senal de salida).
			<-salida
			s := model.Solicitud{IdSolicitud: model.NuevoID(r.cliente), IdCliente: r.cliente, Tipo: model.TipoReserva, IdEvento: *evento, IdOcurrencia: *ocurrencia, CantidadAsientos: cantidad}
			inicio := time.Now()
			r.err = cliente.Pedir(s, &r.resp)
			r.duracion = time.Since(inicio)
			resultados[i] = r
		}(i, cantidad)
	}
	conectados.Wait() // espera a que todos hayan abierto su socket
	// zeromq conecta en segundo plano, este margen asegura que las conexiones tcp
	// ya existan cuando suene la pistola de salida.
	time.Sleep(300 * time.Millisecond)
	fmt.Printf("salen %d clientes al mismo tiempo por %s en la fecha %d, entre todos piden %d asientos\n", len(cantidades), *evento, *ocurrencia, total)
	close(salida) // pistola de salida: suelta a todas las goroutines a la vez
	terminados.Wait()

	// ordena los resultados del que tardo menos al que tardo mas. como el gt
	// atiende por orden de llegada, esto se acerca al orden en que los atendieron.
	// (la funcion entre llaves compara dos elementos a y b.)
	sort.Slice(resultados, func(a, b int) bool { return resultados[a].duracion < resultados[b].duracion }) // orden aproximado de atencion
	concedidos, aceptados := 0, 0
	for _, r := range resultados {
		if r.err != nil {
			fmt.Printf("%s pidio %s, %s: %v\n", r.cliente, model.Asientos(r.pedidos), model.Resultado(false), r.err)
			continue
		}
		if r.resp.Exito {
			concedidos += r.pedidos
			aceptados++
		}
		fmt.Printf("%s pidio %s, %s: %s (%v)\n", r.cliente, model.Asientos(r.pedidos), model.Resultado(r.resp.Exito), r.resp.Mensaje, r.duracion.Round(time.Microsecond))
	}
	fmt.Printf("%s, %s y se vendieron %d de los %d asientos pedidos\n", cuantos(aceptados, "cliente consiguio puesto", "clientes consiguieron puesto"), cuantos(len(resultados)-aceptados, "se quedo sin puesto", "se quedaron sin puesto"), concedidos, total)
	return nil
}

// cuantos arma frases en singular o plural: "1 cliente consiguio puesto" / "3 clientes consiguieron puesto".
func cuantos(n int, uno, varios string) string {
	if n == 1 {
		return "1 " + uno
	}
	return fmt.Sprintf("%d %s", n, varios)
}
