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

type opcionesComunes struct {
	gt      *string
	id      *string
	timeout *time.Duration
}

type resultadoConcurrente struct {
	cliente  string
	pedidos  int
	resp     model.Respuesta
	err      error
	duracion time.Duration
}

func main() {
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
		os.Exit(1)
	}
}

func uso() {
	fmt.Fprintln(os.Stderr, `uso: client <comando> [opciones]

comandos:
  consultar, consulta los eventos de un mes
  reservar, reserva asientos de un evento
  concurrente, lanza varios clientes que reservan al mismo tiempo`)
	os.Exit(2)
}

func opciones(fs *flag.FlagSet, idPorDefecto string) opcionesComunes {
	return opcionesComunes{
		gt:      fs.String("gt", entorno("SRB_GT", "tcp://localhost:5555"), "endpoint del Gestor de Transacciones"),
		id:      fs.String("id", idPorDefecto, "identificador del cliente"),
		timeout: fs.Duration("timeout", 5*time.Second, "cuanto esperar la respuesta"),
	}
}

func entorno(variable, valorPorDefecto string) string {
	if valor := os.Getenv(variable); valor != "" {
		return valor
	}
	return valorPorDefecto
}

func enviar(op opcionesComunes, s model.Solicitud) (model.Respuesta, time.Duration, error) {
	var resp model.Respuesta
	cliente, err := communication.NuevoClienteReq(*op.gt, *op.timeout)
	if err != nil {
		return resp, 0, err
	}
	defer cliente.Cerrar()
	inicio := time.Now()
	err = cliente.Pedir(s, &resp)
	return resp, time.Since(inicio), err
}

func consultar(argumentos []string) error {
	fs := flag.NewFlagSet("consultar", flag.ExitOnError)
	op := opciones(fs, "Cliente1")
	mes := fs.String("mes", "2026-11", "mes a consultar, por ejemplo 2026-11")
	fs.Parse(argumentos)

	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoConsulta, Mes: *mes}
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
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

func concurrente(argumentos []string) error {
	fs := flag.NewFlagSet("concurrente", flag.ExitOnError)
	op := opciones(fs, "Concurrente")
	evento := fs.String("evento", "E2", "evento por el que compiten los clientes")
	ocurrencia := fs.Int("ocurrencia", 1, "cual fecha del evento")
	lista := fs.String("asientos", "30,25,40,20,15", "asientos que pide cada cliente, separados por coma")
	fs.Parse(argumentos)

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

	resultados := make([]resultadoConcurrente, len(cantidades))
	salida := make(chan struct{})
	var conectados, terminados sync.WaitGroup
	for i, cantidad := range cantidades {
		conectados.Add(1)
		terminados.Add(1)
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
			<-salida
			s := model.Solicitud{IdSolicitud: model.NuevoID(r.cliente), IdCliente: r.cliente, Tipo: model.TipoReserva, IdEvento: *evento, IdOcurrencia: *ocurrencia, CantidadAsientos: cantidad}
			inicio := time.Now()
			r.err = cliente.Pedir(s, &r.resp)
			r.duracion = time.Since(inicio)
			resultados[i] = r
		}(i, cantidad)
	}
	conectados.Wait()
	time.Sleep(300 * time.Millisecond)
	fmt.Printf("salen %d clientes al mismo tiempo por %s en la fecha %d, entre todos piden %d asientos\n\n", len(cantidades), *evento, *ocurrencia, total)
	close(salida)
	terminados.Wait()

	sort.Slice(resultados, func(a, b int) bool { return resultados[a].duracion < resultados[b].duracion }) // orden aproximado de atención
	concedidos, aceptados := 0, 0
	fmt.Printf("%-16s %6s %12s  %s\n", "Cliente", "Pidio", "Tiempo", "Resultado")
	for _, r := range resultados {
		if r.err != nil {
			fmt.Printf("%-16s %6d %12s  %s %v\n", r.cliente, r.pedidos, "-", model.Resultado(false), r.err)
			continue
		}
		if r.resp.Exito {
			concedidos += r.pedidos
			aceptados++
		}
		fmt.Printf("%-16s %6d %12v %s %s\n", r.cliente, r.pedidos, r.duracion.Round(time.Microsecond), model.Resultado(r.resp.Exito), r.resp.Mensaje)
	}
	fmt.Printf("\n%s, %s y se vendieron %d de los %d asientos pedidos\n", cuantos(aceptados, "cliente consiguio puesto", "clientes consiguieron puesto"), cuantos(len(resultados)-aceptados, "se quedo sin puesto", "se quedaron sin puesto"), concedidos, total)
	return nil
}

func cuantos(n int, uno, varios string) string {
	if n == 1 {
		return "1 " + uno
	}
	return fmt.Sprintf("%d %s", n, varios)
}
