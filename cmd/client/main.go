// CLIENTE - Máquina 1, capa de consumidores.
//
// Programa de línea de comandos que simula usuarios. Tiene tres "subcomandos":
//
//	client consultar     -> pide al GT la lista de eventos de un mes
//	client reservar      -> pide al GT reservar asientos de un evento
//	client concurrente   -> lanza VARIOS clientes a la vez compitiendo por el mismo evento
//
// El cliente es el que abre la conexión hacia el GT (Connect, hacia el puerto 5555
// de la M2). La respuesta vuelve por esa misma conexión, por eso la M1 NO necesita
// puertos abiertos ni que nadie se conecte a ella (por ahora: la notificación de
// cancelaciones de la segunda entrega sí va a requerir pensar cómo contactar al cliente).
//
// Cada ejecución del programa envía UNA solicitud (o varias en paralelo en
// "concurrente") y termina. NO es todavía un generador de carga continua.
package main

import (
	"flag"
	"fmt"
	"os"
	"sistemaboletos/internal/communication"
	"sistemaboletos/internal/model"
	"sort"
	"strconv" // convertir texto a número (Atoi)
	"strings"
	"sync" // herramientas para coordinar goroutines (WaitGroup)
	"time"
)

// opcionesComunes agrupa las opciones que comparten los tres subcomandos.
// Son punteros porque flag devuelve punteros (se llenan al hacer Parse).
type opcionesComunes struct {
	gt      *string        // dirección del Gestor de Transacciones
	id      *string        // nombre del cliente
	timeout *time.Duration // cuánto esperar la respuesta
}

// resultadoConcurrente guarda lo que le pasó a cada cliente del subcomando concurrente.
type resultadoConcurrente struct {
	cliente  string
	pedidos  int // cuántos asientos pidió
	resp     model.Respuesta
	err      error // si hubo error de comunicación (timeout, etc.)
	duracion time.Duration
}

func main() {
	// os.Args es la lista de palabras escritas en la terminal:
	// os.Args[0] = nombre del programa, os.Args[1] = el subcomando, el resto = opciones.
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
		os.Exit(1) // código de salida 1 = terminó con error
	}
}

// uso imprime la ayuda y termina el programa (código 2 = mal uso).
func uso() {
	fmt.Fprintln(os.Stderr, `uso: client <comando> [opciones]

comandos:
  consultar, consulta los eventos de un mes
  reservar, reserva asientos de un evento
  concurrente, lanza varios clientes que reservan al mismo tiempo`)
	os.Exit(2)
}

// opciones define las tres opciones comunes sobre el conjunto de opciones "fs"
// de un subcomando. Aquí se explican -gt, -id y -timeout:
//
//	-gt       dónde está el GT, con formato tcp://IP:PUERTO (ej. tcp://192.168.1.20:5555).
//	          Si no se escribe, se usa la variable de entorno SRB_GT; si tampoco existe,
//	          tcp://localhost:5555. Orden de prioridad: opción > variable > valor por defecto.
//	-id       nombre de este cliente (queda guardado en la reserva y en los logs).
//	-timeout  cuánto esperar la respuesta antes de rendirse (ej. 5s, 500ms).
func opciones(fs *flag.FlagSet, idPorDefecto string) opcionesComunes {
	return opcionesComunes{
		gt:      fs.String("gt", entorno("SRB_GT", "tcp://localhost:5555"), "endpoint del Gestor de Transacciones"),
		id:      fs.String("id", idPorDefecto, "identificador del cliente"),
		timeout: fs.Duration("timeout", 5*time.Second, "cuanto esperar la respuesta"),
	}
}

// entorno lee una variable de entorno; si no existe o está vacía, usa el valor por defecto.
func entorno(variable, valorPorDefecto string) string {
	if valor := os.Getenv(variable); valor != "" {
		return valor
	}
	return valorPorDefecto
}

// enviar manda UNA solicitud al GT y devuelve: la respuesta, cuánto tardó y un error.
// Observa que Go permite devolver varios valores a la vez.
func enviar(op opcionesComunes, s model.Solicitud) (model.Respuesta, time.Duration, error) {
	var resp model.Respuesta
	// Cada envío crea su propio socket REQ conectado al GT (op.gt es un puntero, de ahí el *).
	cliente, err := communication.NuevoClienteReq(*op.gt, *op.timeout)
	if err != nil {
		return resp, 0, err
	}
	defer cliente.Cerrar()
	// Se mide el tiempo de respuesta (ida y vuelta completa) como la diferencia
	// entre antes y después de Pedir. Esta es la base de las métricas de rendimiento.
	inicio := time.Now()
	err = cliente.Pedir(s, &resp)
	return resp, time.Since(inicio), err
}

// consultar: subcomando "client consultar". Pide los eventos de un mes.
func consultar(argumentos []string) error {
	// Cada subcomando tiene su propio conjunto de opciones (FlagSet).
	// ExitOnError: si las opciones están mal escritas, imprime la ayuda y termina.
	fs := flag.NewFlagSet("consultar", flag.ExitOnError)
	op := opciones(fs, "Cliente1")
	mes := fs.String("mes", "2026-11", "mes a consultar, por ejemplo 2026-11")
	fs.Parse(argumentos)

	// Se arma la solicitud. NuevoID genera un id único ("Cliente1-a1b2c3...").
	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoConsulta, Mes: *mes}
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	// "lo atendio RMC-1": la respuesta trae el nombre de la instancia que la atendió.
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

// reservar: subcomando "client reservar". Pide N asientos de una fecha de un evento.
// Las reservas son por CANTIDAD de asientos (no se elige el número de silla).
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

// concurrente: subcomando "client concurrente". Simula VARIOS clientes que
// piden reservar el MISMO evento exactamente al mismo tiempo.
//
// Para qué sirve: demostrar que el sistema no vende más asientos de los que hay
// aunque muchos clientes compitan. Con -asientos 30,25,40,20,15 se lanzan 5
// clientes que piden 30, 25, 40, 20 y 15 asientos de un evento de 100: en total
// piden 130, así que alguno tiene que quedar sin puesto.
//
// IMPORTANTE: NO es el generador de carga del enunciado (20 solicitudes por
// minuto, parametrizable, durante 2-3 minutos, con consultas y reservas mezcladas).
// Es una demostración de competencia por asientos: cada cliente hace UNA sola
// solicitud, y solo de reserva.
//
// Cómo funciona (la "técnica de la pistola de salida"):
//  1. Se crea una goroutine por cliente. Una goroutine es un hilo ligero de Go
//     (se lanza con la palabra "go"); todas corren a la vez.
//  2. Cada una abre su propio socket REQ (un socket ZeroMQ no se puede compartir
//     entre hilos y un REQ solo admite una petición a la vez).
//  3. Cada una avisa "ya me conecté" y se queda esperando en el canal "salida".
//  4. El hilo principal espera a que todos estén conectados, aguarda 300 ms para
//     que las conexiones TCP terminen de establecerse y entonces cierra el canal
//     (close(salida)): eso despierta a TODAS las goroutines al mismo instante.
//  5. Cada una envía su reserva y mide su tiempo. El hilo principal espera a que
//     terminen todas, ordena los resultados por duración (≈ orden de atención) y
//     muestra quién consiguió asientos y cuántos se vendieron en total.
func concurrente(argumentos []string) error {
	fs := flag.NewFlagSet("concurrente", flag.ExitOnError)
	op := opciones(fs, "Concurrente")
	evento := fs.String("evento", "E2", "evento por el que compiten los clientes")
	ocurrencia := fs.Int("ocurrencia", 1, "cual fecha del evento")
	lista := fs.String("asientos", "30,25,40,20,15", "asientos que pide cada cliente, separados por coma")
	fs.Parse(argumentos)

	// Convierte el texto "30,25,40" en la lista de números [30, 25, 40].
	// strings.Split parte el texto por las comas; Atoi ("ASCII to integer") convierte cada pedazo.
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

	// Lista donde cada goroutine escribe su resultado en SU propia posición
	// (por eso no hay conflicto entre ellas).
	resultados := make([]resultadoConcurrente, len(cantidades))
	// Un canal (chan) es un "tubo" para comunicar goroutines. Este es de tipo
	// struct{} (no transporta datos): solo se usa como señal de "¡ya!".
	salida := make(chan struct{})
	// WaitGroup = contador para esperar a un grupo de goroutines:
	// Add(1) suma una, Done() resta una, Wait() bloquea hasta que llegue a cero.
	// "conectados" espera a que todas hayan abierto su socket; "terminados", a que acaben.
	var conectados, terminados sync.WaitGroup
	for i, cantidad := range cantidades {
		conectados.Add(1)
		terminados.Add(1)
		// "go func(...) {...}(i, cantidad)": lanza la función anónima en una
		// goroutine, pasándole los valores actuales de i y cantidad.
		go func(i, cantidad int) {
			defer terminados.Done()
			r := resultadoConcurrente{cliente: fmt.Sprintf("%s-%d", *op.id, i+1), pedidos: cantidad}
			cliente, err := communication.NuevoClienteReq(*op.gt, *op.timeout) // cada cliente tiene su propio socket REQ
			conectados.Done()
			if err != nil {
				r.err = err
				resultados[i] = r
				return
			}
			defer cliente.Cerrar()
			// "<-salida" = quedarse esperando hasta que el canal se cierre (la señal de salida).
			<-salida
			s := model.Solicitud{IdSolicitud: model.NuevoID(r.cliente), IdCliente: r.cliente, Tipo: model.TipoReserva, IdEvento: *evento, IdOcurrencia: *ocurrencia, CantidadAsientos: cantidad}
			inicio := time.Now()
			r.err = cliente.Pedir(s, &r.resp)
			r.duracion = time.Since(inicio)
			resultados[i] = r
		}(i, cantidad)
	}
	conectados.Wait() // espera a que todos hayan abierto su socket
	// ZeroMQ conecta en segundo plano: este pequeño margen asegura que las
	// conexiones TCP ya existan cuando suene la "pistola de salida".
	time.Sleep(300 * time.Millisecond)
	fmt.Printf("salen %d clientes al mismo tiempo por %s en la fecha %d, entre todos piden %d asientos\n", len(cantidades), *evento, *ocurrencia, total)
	close(salida) // ¡pistola de salida! libera a todas las goroutines a la vez
	terminados.Wait()

	// Ordena los resultados del que tardó menos al que tardó más. Como el GT
	// atiende en orden de llegada, esto aproxima el orden en que se atendieron.
	// (La función entre llaves compara dos elementos a y b.)
	sort.Slice(resultados, func(a, b int) bool { return resultados[a].duracion < resultados[b].duracion }) // orden aproximado de atención
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

// cuantos arma frases con singular/plural: "1 cliente consiguio puesto" / "3 clientes consiguieron puesto".
func cuantos(n int, uno, varios string) string {
	if n == 1 {
		return "1 " + uno
	}
	return fmt.Sprintf("%d %s", n, varios)
}
