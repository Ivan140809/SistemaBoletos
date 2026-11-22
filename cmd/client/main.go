// cliente de la maquina 1, programa de terminal que simula usuarios. ademas de los subcomandos de
// abajo tiene carga y analizar para los experimentos de desempeno, estan en carga.go.
// consultar pide los eventos de un mes, reservar pide asientos, modificar cambia los asientos o la
// fecha de una reserva, cancelar la cancela y concurrente lanza varios clientes a la vez peleando
// por el mismo evento. el cliente hace connect al gt y la respuesta vuelve por esa misma conexion.
// la unica excepcion es cancelar, que abre un pull con bind para recibir la notificacion despues,
// por eso la m1 tiene que dejar entrar conexiones desde la m2 cuando se cancela.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"net"
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
	gt *string // direccion del gestor de transacciones
	id *string // nombre del cliente
	timeout *time.Duration // cuanto esperar la respuesta
	reintentos *int // TOLERANCIA A FALLAS, cuantas veces reenviar si no hay respuesta
}

// resultadoConcurrente guarda lo que le paso a cada cliente del subcomando concurrente
type resultadoConcurrente struct {
	cliente string // nombre del cliente
	pedidos int // cuantos asientos pidio
	resp model.Respuesta // lo que contesto el sistema
	err error // error de comunicacion si hubo
	duracion time.Duration // cuanto tardo la respuesta
}

// main lee el subcomando y llama a la funcion que toca
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
	case "modificar":
		err = modificar(argumentos)
	case "cancelar":
		err = cancelar(argumentos)
	case "carga":
		err = carga(argumentos)
	case "analizar":
		err = analizar(argumentos)
	default:
		uso()
	}
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
  concurrente, lanza varios clientes que reservan al mismo tiempo
  modificar, cambia los asientos o la fecha de una reserva
  cancelar, cancela una reserva y espera la notificacion
  carga, genera carga con varios clientes y guarda cada solicitud en un csv
  analizar, saca promedio, desviacion, p95 y transacciones por minuto de los csv de carga`)
	os.Exit(2)
}

// opciones define -gt -id y -timeout, -gt usa SRB_GT si no se escribe y si no localhost
func opciones(fs *flag.FlagSet, idPorDefecto string) opcionesComunes {
	return opcionesComunes{
		gt: fs.String("gt", entorno("SRB_GT", "tcp://localhost:5555"), "endpoint del Gestor de Transacciones, o primario y respaldo separados por coma"),
		id: fs.String("id", idPorDefecto, "identificador del cliente"),
		timeout: fs.Duration("timeout", 5*time.Second, "cuanto esperar la respuesta"),
		// TOLERANCIA A FALLAS, patron lazy pirate, reenvia la misma solicitud si no llega respuesta
		reintentos: fs.Int("reintentos", 3, "cuantas veces reenviar la solicitud si no llega respuesta"),
	}
}

// entorno lee una variable de entorno y si esta vacia usa el valor por defecto
func entorno(variable, valorPorDefecto string) string {
	if valor := os.Getenv(variable); valor != "" {
		return valor
	}
	return valorPorDefecto
}

// nuevoCliente crea el socket req hacia el gt con los reintentos configurados
func nuevoCliente(op opcionesComunes, nombre string) (*communication.ClienteReq, error) {
	// TOLERANCIA A FALLAS, -gt puede traer el gt primario y el de respaldo separados por coma,
	// se usa uno a la vez y si no contesta el reintento va al otro
	cliente, err := communication.NuevoClienteFailover(*op.gt, *op.timeout)
	if err != nil {
		return nil, err
	}
	// TOLERANCIA A FALLAS, si el gt no contesta a tiempo se reenvia la misma solicitud con el mismo id,
	// la base la reconoce y no la ejecuta dos veces
	cliente.Reintentos = *op.reintentos
	// TOLERANCIA A FALLAS DEL GT, si contesta un gt pasivo se pasa de una vez al otro gt
	cliente.Rechazar = func(datos []byte) bool {
		var r model.Respuesta
		json.Unmarshal(datos, &r)
		return r.Instancia == model.GTPasivo
	}
	cliente.AlReintentar = func(intento int, err error) {
		fmt.Fprintf(os.Stderr, "%s: %v, reintento %d de %d en %s\n", nombre, err, intento, *op.reintentos, cliente.Direccion())
	}
	return cliente, nil
}

// enviar manda una solicitud al gt y devuelve la respuesta, cuanto tardo y un error
func enviar(op opcionesComunes, s model.Solicitud) (model.Respuesta, time.Duration, error) {
	var resp model.Respuesta
	// aqui se crea el socket req propio de este envio
	cliente, err := nuevoCliente(op, s.IdCliente)
	if err != nil {
		return resp, 0, err
	}
	defer cliente.Cerrar()
	inicio := time.Now()
	err = cliente.Pedir(s, &resp)
	return resp, time.Since(inicio), err
}

// consultar es el subcomando que pide los eventos de un mes
func consultar(argumentos []string) error {
	fs := flag.NewFlagSet("consultar", flag.ExitOnError)
	op := opciones(fs, "Cliente1")
	mes := fs.String("mes", "2026-11", "mes a consultar, por ejemplo 2026-11")
	fs.Parse(argumentos)

	// aqui se crea la solicitud de consulta con un id unico
	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoConsulta, Mes: *mes}
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	// ESCALABILIDAD HORIZONTAL, muestra cual instancia de rmc atendio la solicitud
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

// reservar es el subcomando que pide n asientos de una fecha de un evento
func reservar(argumentos []string) error {
	fs := flag.NewFlagSet("reservar", flag.ExitOnError)
	op := opciones(fs, "Cliente1")
	evento := fs.String("evento", "E1", "identificador del evento")
	ocurrencia := fs.Int("ocurrencia", 1, "cual fecha del evento, 1 o 2")
	asientos := fs.Int("asientos", 1, "cantidad de asientos")
	fs.Parse(argumentos)

	// aqui se crea la solicitud de reserva con un id unico
	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoReserva, IdEvento: *evento, IdOcurrencia: *ocurrencia, CantidadAsientos: *asientos}
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	fmt.Printf("%s pidio %s de %s en la fecha %d, %s: %s (%v)\n", *op.id, model.Asientos(*asientos), *evento, *ocurrencia, model.Resultado(resp.Exito), resp.Mensaje, duracion.Round(time.Microsecond))
	return nil
}

//modificar es el subcomando que cambia los asientos o la fecha de una reserva

func modificar(argumentos []string) error {
	// aqui se crean las opciones de este subcomando
	fs := flag.NewFlagSet("modificar", flag.ExitOnError)
	// opciones comunes, el id debe ser el mismo cliente que hizo la reserva
	op := opciones(fs, "Cliente1")
	// reserva a modificar
	reserva := fs.String("reserva", "", "reserva a modificar, por ejemplo R0001")
	// nuevo total de asientos
	asientos := fs.Int("asientos", 0, "nuevo total de asientos de la reserva")
	// nueva fecha del evento
	ocurrencia := fs.Int("ocurrencia", 0, "nueva fecha del evento, 1 o 2, conserva los asientos")
	// lee las opciones
	fs.Parse(argumentos)

	// aqui se crea la solicitud de modificacion con un id unico
	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoModificacion, IdReserva: *reserva, NuevaCantidad: *asientos, NuevaOcurrencia: *ocurrencia}
	// la manda al gt
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	// imprime el resultado y el tiempo
	fmt.Printf("%s, %s: %s (%v)\n", s.Resumen(), model.Resultado(resp.Exito), resp.Mensaje, duracion.Round(time.Microsecond))
	return nil
}

// cancelar es el subcomando que cancela una reserva. es asincrono, el gt responde de una vez que
// la recibio y el resultado real llega despues como notificacion al pull que abre este cliente
func cancelar(argumentos []string) error {
	// aqui se crean las opciones de este subcomando
	fs := flag.NewFlagSet("cancelar", flag.ExitOnError)
	// opciones comunes, el id debe ser el mismo cliente que hizo la reserva
	op := opciones(fs, "Cliente1")
	// reserva a cancelar
	reserva := fs.String("reserva", "", "reserva a cancelar, por ejemplo R0001")
	// ip de esta maquina que ve la m2, si no se da se averigua sola
	ip := fs.String("ip", "", "ip de esta maquina para recibir la notificacion, por defecto se averigua")
	// puerto donde se espera la notificacion, 0 es cualquiera libre
	puerto := fs.Int("puerto", 0, "puerto donde se espera la notificacion, 0 para uno libre")
	// cuanto esperar la notificacion
	espera := fs.Duration("espera", 15*time.Second, "cuanto esperar la notificacion")
	// lee las opciones
	fs.Parse(argumentos)

	// aqui se crea el pull donde llega la notificacion, hace bind porque el servicio de
	// cancelaciones es el que se conecta a este cliente
	buzon, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		return err
	}
	// cierra el socket al terminar
	defer buzon.Close()
	// si no se espera nada mas no se queda colgado al cerrar
	buzon.SetLinger(0)
	// con puerto 0 zeromq escoge uno libre
	escucha := fmt.Sprintf("tcp://*:%d", *puerto)
	if *puerto == 0 {
		escucha = "tcp://*:*"
	}
	if err := buzon.Bind(escucha); err != nil {
		return fmt.Errorf("no se pudo abrir el puerto para la notificacion: %w", err)
	}
	// averigua en que puerto quedo escuchando
	ultimo, _ := buzon.GetLastEndpoint()
	_, puertoReal, _ := net.SplitHostPort(strings.TrimPrefix(ultimo, "tcp://"))
	// averigua la ip que ve la m2 si no se dio
	if *ip == "" {
		*ip = ipHacia(*op.gt)
	}
	// aqui se crea la direccion que se manda en la solicitud
	direccion := fmt.Sprintf("tcp://%s:%s", *ip, puertoReal)

	// aqui se crea la solicitud de cancelacion con un id unico y la direccion de notificacion
	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoCancelacion, IdReserva: *reserva, DireccionNotificacion: direccion}
	// la manda al gt, que responde de una vez
	resp, duracion, err := enviar(op, s)
	if err != nil {
		return err
	}
	// imprime la confirmacion de recepcion
	fmt.Printf("%s, %s: %s (%v)\n", s.Resumen(), model.Resultado(resp.Exito), resp.Mensaje, duracion.Round(time.Microsecond))
	// si el gt la rechazo no llega ninguna notificacion
	if !resp.Exito {
		return nil
	}

	// se queda esperando la notificacion sin bloquear para siempre
	buzon.SetRcvtimeo(*espera)
	fmt.Printf("esperando la notificacion en %s\n", direccion)
	datos, err := buzon.RecvBytes(0)
	if err != nil {
		return fmt.Errorf("no llego la notificacion en %v, revise que la m2 pueda conectarse a %s", *espera, direccion)
	}
	// aqui se crea la respuesta a partir de la notificacion
	var notificacion model.Respuesta
	if err := json.Unmarshal(datos, &notificacion); err != nil {
		return fmt.Errorf("llego una notificacion que no se entiende: %w", err)
	}
	// imprime el resultado real de la cancelacion
	fmt.Printf("notificacion para %s, %s: %s\n", *op.id, model.Resultado(notificacion.Exito), notificacion.Mensaje)
	return nil
}

// ipHacia averigua la ip de esta maquina en la red por donde se llega al gt.
// no manda nada, solo le pregunta al sistema operativo que tarjeta usaria
func ipHacia(gt string) string {
	// con primario y respaldo basta el primero, los dos estan en la red que ve la m2
	gt = strings.Split(gt, ",")[0]
	// saca host y puerto de tcp://host:puerto
	direccion := strings.TrimPrefix(strings.TrimSpace(gt), "tcp://")
	// udp no abre conexion de verdad, solo escoge la ruta, udp4 para que localhost no de ::1
	conexion, err := net.Dial("udp4", direccion)
	if err != nil {
		return "127.0.0.1"
	}
	defer conexion.Close()
	// la ip local de esa ruta
	host, _, _ := net.SplitHostPort(conexion.LocalAddr().String())
	return host
}

// concurrente lanza varios clientes que reservan el mismo evento en el mismo instante.
// demuestra que no se venden mas asientos de los que hay aunque todos compitan a la vez
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
	// aqui se crea el canal salida que suelta a todos los clientes a la vez
	salida := make(chan struct{})
	var conectados, terminados sync.WaitGroup
	for i, cantidad := range cantidades {
		conectados.Add(1)
		terminados.Add(1)
		// aqui se crea la goroutine de un cliente
		go func(i, cantidad int) {
			defer terminados.Done()
			r := resultadoConcurrente{cliente: fmt.Sprintf("%s-%d", *op.id, i+1), pedidos: cantidad}
			// aqui se crea el socket req propio de este cliente
			cliente, err := nuevoCliente(op, r.cliente)
			conectados.Done()
			if err != nil {
				r.err = err
				resultados[i] = r
				return
			}
			defer cliente.Cerrar()
			<-salida
			// aqui se crea la solicitud de reserva
			s := model.Solicitud{IdSolicitud: model.NuevoID(r.cliente), IdCliente: r.cliente, Tipo: model.TipoReserva, IdEvento: *evento, IdOcurrencia: *ocurrencia, CantidadAsientos: cantidad}
			inicio := time.Now()
			r.err = cliente.Pedir(s, &r.resp)
			r.duracion = time.Since(inicio)
			resultados[i] = r
		}(i, cantidad)
	}
	conectados.Wait()
	time.Sleep(300 * time.Millisecond)
	fmt.Printf("salen %d clientes al mismo tiempo por %s en la fecha %d, entre todos piden %d asientos\n", len(cantidades), *evento, *ocurrencia, total)
	close(salida)
	terminados.Wait()

	sort.Slice(resultados, func(a, b int) bool { return resultados[a].duracion < resultados[b].duracion })
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

// cuantos arma la frase en singular o plural segun n
func cuantos(n int, uno, varios string) string {
	if n == 1 {
		return "1 " + uno
	}
	return fmt.Sprintf("%d %s", n, varios)
}
