package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	zmq "github.com/pebbe/zmq4"
	"sistemaboletos/internal/communication"
	"sistemaboletos/internal/model"
)

type opcionesComunes struct {
	gt *string
	id string
	timeout *time.Duration
}

type resultadoConcurrente struct {
	cliente string
	pedidos int
	resp model.Respuesta
	err error
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
	case "cancelar":
		err = cancelar(argumentos)
	case "concurrente":
		err = concurrente(argumentos)
	case "carga":
		err = carga(argumentos)
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
  consultar consulta los eventos de un mes
  reservar reserva asientos de un evento
  cancelar cancela una reserva y espera la notificación
  concurrente lanza varios clientes que reservan al mismo tiempo
  carga generador de carga parametrizable (archivo JSON)

use "client <comando> -h" para ver las opciones de cada comando`)
	os.Exit(2)
}

func opciones(fs *flag.FlagSet, idPorDefecto string) opcionesComunes {
	return opcionesComunes{
		gt: fs.String("gt", entorno("SRB_GT", "tcp://localhost:5555"), "endpoint del Gestor de Transacciones"),
		id: fs.String("id", idPorDefecto, "identificador del cliente"),
		timeout: fs.Duration("timeout", 5*time.Second, "tiempo máximo de espera por la respuesta"),
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
	mes := fs.String("mes", "2026-11", "mes a consultar (AAAA-MM)")
	fs.Parse(argumentos)

	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoConsulta, Mes: *mes}
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	fmt.Printf("[%s] CONSULTA mes=%s -> %s %s (%v, %s)\n", *op.id, *mes, model.Resultado(resp.Exito), resp.Mensaje, duracion.Round(time.Microsecond), resp.Instancia)
	if resp.Exito {
		fmt.Print(model.TablaEventos(resp.Eventos))
	}
	return nil
}

func reservar(argumentos []string) error {
	fs := flag.NewFlagSet("reservar", flag.ExitOnError)
	op := opciones(fs, "Cliente1")
	evento := fs.String("evento", "E1", "identificador del evento")
	ocurrencia := fs.Int("ocurrencia", 1, "ocurrencia del evento en el mes (1 o 2)")
	asientos := fs.Int("asientos", 1, "cantidad de asientos")
	fs.Parse(argumentos)

	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoReserva, IdEvento: *evento, IdOcurrencia: *ocurrencia, CantidadAsientos: *asientos}
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	fmt.Printf("[%s] RESERVA %d asientos en %s/%d -> %s %s (%v)\n", *op.id, *asientos, *evento, *ocurrencia, model.Resultado(resp.Exito), resp.Mensaje, duracion.Round(time.Microsecond))
	return nil
}

func cancelar(argumentos []string) error {
	fs := flag.NewFlagSet("cancelar", flag.ExitOnError)
	op := opciones(fs, "Cliente1")
	reserva := fs.String("reserva", "", "identificador de la reserva (ej: R0001)")
	ip := fs.String("ipnotif", entorno("SRB_IP_CLIENTE", "localhost"), "IP de esta máquina, a donde llega la notificación")
	puerto := fs.Int("puertonotif", 6000, "puerto local donde se recibe la notificación")
	espera := fs.Duration("espera", 15*time.Second, "tiempo máximo esperando la notificación")
	fs.Parse(argumentos)

	notificaciones, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		return err
	}
	defer notificaciones.Close()
	notificaciones.SetRcvtimeo(*espera)
	if err := notificaciones.Bind(fmt.Sprintf("tcp://*:%d", *puerto)); err != nil { // el cliente abre su propio puerto para la notificación
		return fmt.Errorf("no se pudo abrir el puerto %d: %w", *puerto, err)
	}

	s := model.Solicitud{
		IdSolicitud:           model.NuevoID(*op.id),
		IdCliente:             *op.id,
		Tipo:                  model.TipoCancelacion,
		IdReserva:             *reserva,
		DireccionNotificacion: fmt.Sprintf("tcp://%s:%d", *ip, *puerto),
	}
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	fmt.Printf("[%s] CANCELACION %s -> %s %s (%v)\n", *op.id, *reserva, model.Resultado(resp.Exito), resp.Mensaje, duracion.Round(time.Microsecond))
	if !resp.Exito {
		return nil
	}
	fmt.Printf("[%s] esperando notificación en %s ...\n", *op.id, s.DireccionNotificacion)
	datos, err := notificaciones.RecvBytes(0)
	if err != nil {
		return fmt.Errorf("no llegó la notificación en %v", *espera)
	}
	var notificacion model.Respuesta
	if err := json.Unmarshal(datos, &notificacion); err != nil {
		return err
	}
	fmt.Printf(" Notificacion\n", *op.id, model.Resultado(notificacion.Exito), notificacion.Mensaje)
	return nil
}

func concurrente(argumentos []string) error {
	fs := flag.NewFlagSet("concurrente", flag.ExitOnError)
	op := opciones(fs, "Concurrente")
	evento := fs.String("evento", "E2", "evento por el que compiten los clientes")
	ocurrencia := fs.Int("ocurrencia", 1, "ocurrencia del evento")
	lista := fs.String("asientos", "30,25,40,20,15", "asientos que pide cada cliente, separados por coma (un cliente por valor)")
	fs.Parse(argumentos)

	var cantidades []int
	total := 0
	for _, valor := range strings.Split(*lista, ",") {
		cantidad, err := strconv.Atoi(strings.TrimSpace(valor))
		if err != nil || cantidad < 1 {
			return fmt.Errorf("cantidad inválida %q", valor)
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
			<-salida // todos esperan la misma señal para arrancar al mismo tiempo
			s := model.Solicitud{IdSolicitud: model.NuevoID(r.cliente), IdCliente: r.cliente, Tipo: model.TipoReserva, IdEvento: *evento, IdOcurrencia: *ocurrencia, CantidadAsientos: cantidad}
			inicio := time.Now()
			r.err = cliente.Pedir(s, &r.resp)
			r.duracion = time.Since(inicio)
			resultados[i] = r
		}(i, cantidad)
	}
	conectados.Wait()
	time.Sleep(300 * time.Millisecond) // margen para que todas las conexiones TCP queden establecidas
	fmt.Printf("Lanzando %d clientes AL MISMO TIEMPO sobre %s/%d: piden %d asientos en total\n\n", len(cantidades), *evento, *ocurrencia, total)
	close(salida)
	terminados.Wait()

	sort.Slice(resultados, func(a, b int) bool { return resultados[a].duracion < resultados[b].duracion }) // orden aproximado de atención
	concedidos, aceptados := 0, 0
	fmt.Printf("%-16s %6s %12s  %s\n", "CLIENTE", "PIDIÓ", "TIEMPO", "RESULTADO")
	for _, r := range resultados {
		if r.err != nil {
			fmt.Printf("%-16s %6d %12s  %s %v\n", r.cliente, r.pedidos, "-", model.Resultado(false), r.err)
			continue
		}
		if r.resp.Exito {
			concedidos += r.pedidos
			aceptados++
		}
		fmt.Printf("%-16s %6d %12v  %s %s\n", r.cliente, r.pedidos, r.duracion.Round(time.Microsecond), model.Resultado(r.resp.Exito), r.resp.Mensaje)
	}
	fmt.Printf("\nClientes con reserva: %d | clientes rechazados: %d | asientos concedidos: %d de %d pedidos\n", aceptados, len(resultados)-aceptados, concedidos, total)
	return nil
}
