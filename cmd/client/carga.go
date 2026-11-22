// generador de carga y analisis de metricas del cliente, segunda entrega.
// carga lanza varios clientes a la vez, cada uno en su goroutine con su propio socket req hacia el gt,
// durante un tiempo fijo y a una tasa fija, y guarda cada solicitud en un csv.
// analizar lee uno o varios csv y saca promedio, desviacion estandar, p95 y transacciones por minuto.
// ya se viene armado con los escenarios del enunciado
package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sistemaboletos/internal/communication"
	"sistemaboletos/internal/model"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// perfil dice que hace un cliente, cuantos clientes lo usan y con que peso escoge cada operacion
type perfil struct {
	nombre string
	clientes int
	pesos map[model.TipoSolicitud]int
}

// escenarios de la tabla 1 del enunciado
var escenarios = map[int][]perfil{
	1: {
		{nombre: "solo-consulta", clientes: 2, pesos: map[model.TipoSolicitud]int{model.TipoConsulta: 100}},
		{nombre: "consulta-reserva", clientes: 1, pesos: map[model.TipoSolicitud]int{model.TipoConsulta: 50, model.TipoReserva: 50}},
	},
	2: {
		{nombre: "solo-consulta", clientes: 3, pesos: map[model.TipoSolicitud]int{model.TipoConsulta: 100}},
		{nombre: "reserva-modificacion", clientes: 2, pesos: map[model.TipoSolicitud]int{model.TipoReserva: 50, model.TipoModificacion: 50}},
	},
	3: {
		{nombre: "solo-consulta", clientes: 4, pesos: map[model.TipoSolicitud]int{model.TipoConsulta: 100}},
		{nombre: "reserva-modificacion", clientes: 4, pesos: map[model.TipoSolicitud]int{model.TipoReserva: 50, model.TipoModificacion: 50}},
	},
}

// fila es una solicitud medida, una linea del csv
type fila struct {
	etiqueta string // por ejemplo base o escalable
	escenario int
	cliente string
	perfil string
	operacion model.TipoSolicitud
	idSolicitud string
	inicio time.Time
	desdeInicio time.Duration // cuanto despues del arranque de la prueba
	duracion time.Duration
	exito bool // la operacion se hizo
	errorRed bool // no llego respuesta ni con los reintentos
	instancia string // que rmc la atendio
	calentamiento bool // cae en el periodo de calentamiento y no cuenta en las metricas
	mensaje string
}

// reservaPropia es una reserva que hizo este cliente, para poder modificarla despues
type reservaPropia struct {
	id string
	evento string
	ocurrencia int
	cantidad int
}

// carga es el subcomando que genera carga con varios clientes a la vez
func carga(argumentos []string) error {
	fs := flag.NewFlagSet("carga", flag.ExitOnError)
	op := opciones(fs, "Carga")
	escenario := fs.Int("escenario", 1, "escenario de la tabla 1 (1, 2 o 3), 0 para uno propio con -clientes y -mezcla")
	clientes := fs.Int("clientes", 3, "con -escenario 0, cuantos clientes lanzar")
	mezcla := fs.String("mezcla", "consulta=50,reserva=50", "con -escenario 0, porcentaje de cada operacion: consulta, reserva y modificacion")
	duracion := fs.Duration("duracion", 2*time.Minute, "cuanto dura la prueba, el enunciado pide 2 a 3 minutos")
	tasa := fs.Int("tasa", 20, "solicitudes minimas por minuto de cada cliente (el enunciado pide 20), 0 para sin pausa")
	calentamiento := fs.Duration("calentamiento", 15*time.Second, "tiempo inicial que no cuenta en las metricas")
	semilla := fs.Int64("semilla", 1, "semilla del azar, la misma semilla repite la misma carga")
	mes := fs.String("mes", "2026-11", "mes de los eventos")
	etiqueta := fs.String("etiqueta", "base", "nombre de la arquitectura medida, va en el csv (base o escalable)")
	salida := fs.String("csv", "", "archivo csv de resultados, por defecto metrics/<etiqueta>-e<escenario>.csv")
	fs.Parse(argumentos)

	// arma la lista de perfiles segun el escenario
	perfiles, ok := escenarios[*escenario]
	if *escenario == 0 {
		pesos, err := leerMezcla(*mezcla)
		if err != nil {
			return err
		}
		perfiles, ok = []perfil{{nombre: "propio", clientes: *clientes, pesos: pesos}}, true
	}
	if !ok {
		return fmt.Errorf("no existe el escenario %d, use 1, 2, 3 o 0", *escenario)
	}
	if *tasa != 0 && *tasa < 20 {
		fmt.Fprintf(os.Stderr, "aviso: el enunciado pide al menos 20 solicitudes por minuto por cliente y -tasa es %d\n", *tasa)
	}
	if *salida == "" {
		*salida = filepath.Join("metrics", fmt.Sprintf("%s-e%d.csv", *etiqueta, *escenario))
	}

	// consulta una vez los eventos del mes para saber que se puede reservar
	eventos, err := eventosDelMes(op, *mes)
	if err != nil {
		return err
	}

	// abre el csv y una goroutine que escribe las filas en orden de llegada
	archivo, err := crearCSV(*salida)
	if err != nil {
		return err
	}
	defer archivo.Close()
	escritor := csv.NewWriter(archivo)
	escritor.Write([]string{"etiqueta", "escenario", "cliente", "perfil", "operacion", "id_solicitud", "inicio", "desde_inicio_ms", "duracion_ms", "exito", "error_red", "instancia", "calentamiento", "mensaje"})
	filas := make(chan fila, 1024)
	var todas []fila
	escrito := make(chan struct{})
	go func() {
		for f := range filas {
			todas = append(todas, f)
			escritor.Write(f.registro())
		}
		escritor.Flush()
		close(escrito)
	}()

	// cada cliente abre su socket antes de empezar, asi todos arrancan al mismo tiempo
	salidaComun := make(chan struct{})
	var listos, terminados sync.WaitGroup
	total := 0
	for _, p := range perfiles {
		for i := 0; i < p.clientes; i++ {
			total++
			nombre := fmt.Sprintf("%s%d", *op.id, total)
			// cada cliente tiene su propio azar derivado de la semilla, asi la carga se repite igual
			azar := rand.New(rand.NewSource(*semilla + int64(total)))
			listos.Add(1)
			terminados.Add(1)
			go func(nombre string, p perfil, azar *rand.Rand) {
				defer terminados.Done()
				cliente, err := nuevoCliente(op, nombre)
				listos.Done()
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s no pudo conectarse: %v\n", nombre, err)
					return
				}
				defer cliente.Cerrar()
				<-salidaComun
				generar(cliente, nombre, p, azar, eventos, *mes, *tasa, filas)
			}(nombre, p, azar)
		}
	}
	listos.Wait()

	// la prueba empieza para todos en el mismo instante
	fmt.Printf("carga %s escenario %d: %d clientes durante %v a %s, calentamiento %v, csv %s\n", *etiqueta, *escenario, total, *duracion, describirTasa(*tasa), *calentamiento, *salida)
	inicioPrueba = time.Now()
	finPrueba = inicioPrueba.Add(*duracion)
	finCalentamiento = inicioPrueba.Add(*calentamiento)
	etiquetaPrueba, escenarioPrueba = *etiqueta, *escenario
	close(salidaComun)
	terminados.Wait()
	close(filas)
	<-escrito
	if err := escritor.Error(); err != nil {
		return err
	}

	// resumen en pantalla con las mismas cuentas de analizar
	fmt.Println()
	imprimirResumen(resumir(todas, efectivoDe(todas)))
	return nil
}

// valores de la prueba que comparten todas las goroutines, se fijan antes de arrancar
var (
	inicioPrueba, finPrueba, finCalentamiento time.Time
	etiquetaPrueba string
	escenarioPrueba int
)

// generar es lo que hace un cliente durante la prueba
func generar(cliente *communication.ClienteReq, nombre string, p perfil, azar *rand.Rand, eventos []model.Evento, mes string, tasa int, filas chan<- fila) {
	var propias []reservaPropia
	siguiente := time.Now()
	for time.Now().Before(finPrueba) {
		// espera su turno segun la tasa. el intervalo varia entre 70% y 95% del nominal para que los
		// clientes no vayan sincronizados y nunca bajen de la tasa pedida (el enunciado pide minimo 20)
		if tasa > 0 {
			if espera := time.Until(siguiente); espera > 0 {
				time.Sleep(espera)
			}
			intervalo := time.Duration(float64(time.Minute) / float64(tasa) * (0.70 + 0.25*azar.Float64()))
			siguiente = siguiente.Add(intervalo)
			// si va atrasado no intenta ponerse al dia de golpe
			if siguiente.Before(time.Now()) {
				siguiente = time.Now()
			}
			if !time.Now().Before(finPrueba) {
				return
			}
		}
		s, tipo := armarSolicitud(nombre, p, azar, eventos, mes, propias)
		inicio := time.Now()
		var resp model.Respuesta
		err := cliente.Pedir(s, &resp)
		f := fila{
			etiqueta: etiquetaPrueba, escenario: escenarioPrueba, cliente: nombre, perfil: p.nombre, operacion: tipo,
			idSolicitud: s.IdSolicitud, inicio: inicio, desdeInicio: inicio.Sub(inicioPrueba), duracion: time.Since(inicio),
			exito: err == nil && resp.Exito, errorRed: err != nil, instancia: resp.Instancia,
			calentamiento: inicio.Before(finCalentamiento), mensaje: resp.Mensaje,
		}
		if err != nil {
			f.mensaje = err.Error()
		}
		filas <- f
		// lleva la cuenta de sus reservas para poder modificarlas
		if f.exito && resp.Reserva != nil {
			propias = actualizar(propias, *resp.Reserva)
		}
	}
}

// armarSolicitud escoge la operacion segun los pesos del perfil y arma la solicitud
func armarSolicitud(nombre string, p perfil, azar *rand.Rand, eventos []model.Evento, mes string, propias []reservaPropia) (model.Solicitud, model.TipoSolicitud) {
	tipo := escoger(p.pesos, azar)
	// sin reservas propias todavia no hay nada que modificar, se reserva primero
	if tipo == model.TipoModificacion && len(propias) == 0 {
		tipo = model.TipoReserva
	}
	s := model.Solicitud{IdSolicitud: model.NuevoID(nombre), IdCliente: nombre, Tipo: tipo}
	switch tipo {
	case model.TipoConsulta:
		s.Mes = mes
	case model.TipoReserva:
		// un asiento de un evento y fecha al azar
		e := eventos[azar.Intn(len(eventos))]
		o := e.Ocurrencias[azar.Intn(len(e.Ocurrencias))]
		s.IdEvento, s.IdOcurrencia, s.CantidadAsientos = e.IdEvento, o.IdOcurrencia, 1
	case model.TipoModificacion:
		r := propias[azar.Intn(len(propias))]
		s.IdReserva = r.id
		// si el evento tiene dos fechas, la mitad de las veces cambia la fecha
		if otra := otraFecha(eventos, r); otra > 0 && azar.Intn(2) == 0 {
			s.NuevaOcurrencia = otra
		} else {
			// si no, alterna entre 1 y 2 asientos
			s.NuevaCantidad = 3 - r.cantidad
			if s.NuevaCantidad < 1 {
				s.NuevaCantidad = 1
			}
		}
	}
	return s, tipo
}

// escoger saca una operacion al azar segun los pesos
func escoger(pesos map[model.TipoSolicitud]int, azar *rand.Rand) model.TipoSolicitud {
	// orden fijo para que la misma semilla de la misma secuencia
	orden := []model.TipoSolicitud{model.TipoConsulta, model.TipoReserva, model.TipoModificacion}
	total := 0
	for _, t := range orden {
		total += pesos[t]
	}
	n := azar.Intn(total)
	for _, t := range orden {
		if n < pesos[t] {
			return t
		}
		n -= pesos[t]
	}
	return model.TipoConsulta
}

// otraFecha devuelve la otra fecha del evento de la reserva, 0 si el evento tiene una sola
func otraFecha(eventos []model.Evento, r reservaPropia) int {
	for _, e := range eventos {
		if e.IdEvento != r.evento || len(e.Ocurrencias) < 2 {
			continue
		}
		for _, o := range e.Ocurrencias {
			if o.IdOcurrencia != r.ocurrencia {
				return o.IdOcurrencia
			}
		}
	}
	return 0
}

// actualizar agrega o cambia una reserva propia con lo que devolvio el sistema
func actualizar(propias []reservaPropia, r model.Reserva) []reservaPropia {
	nueva := reservaPropia{id: r.IdReserva, evento: r.IdEvento, ocurrencia: r.IdOcurrencia, cantidad: r.CantidadAsientos}
	for i := range propias {
		if propias[i].id == r.IdReserva {
			propias[i] = nueva
			return propias
		}
	}
	return append(propias, nueva)
}

// eventosDelMes hace una consulta inicial para saber que eventos hay, no se mide
func eventosDelMes(op opcionesComunes, mes string) ([]model.Evento, error) {
	s := model.Solicitud{IdSolicitud: model.NuevoID(*op.id), IdCliente: *op.id, Tipo: model.TipoConsulta, Mes: mes}
	resp, _, err := enviar(op, s)
	if err != nil {
		return nil, fmt.Errorf("no se pudieron consultar los eventos antes de la prueba: %w", err)
	}
	var eventos []model.Evento
	for _, e := range resp.Eventos {
		if len(e.Ocurrencias) > 0 {
			eventos = append(eventos, e)
		}
	}
	if len(eventos) == 0 {
		return nil, fmt.Errorf("no hay eventos en %s, cargue la base antes de la prueba", mes)
	}
	return eventos, nil
}

// leerMezcla convierte "consulta=50,reserva=30,modificacion=20" en pesos
func leerMezcla(texto string) (map[model.TipoSolicitud]int, error) {
	pesos := map[model.TipoSolicitud]int{}
	total := 0
	for _, parte := range strings.Split(texto, ",") {
		nombre, valor, ok := strings.Cut(strings.TrimSpace(parte), "=")
		n, err := strconv.Atoi(valor)
		if !ok || err != nil || n < 0 {
			return nil, fmt.Errorf("la mezcla %q no sirve, escribala como consulta=50,reserva=30,modificacion=20", texto)
		}
		tipo := model.TipoSolicitud(nombre)
		if tipo != model.TipoConsulta && tipo != model.TipoReserva && tipo != model.TipoModificacion {
			return nil, fmt.Errorf("la operacion %q no se puede generar, use consulta, reserva o modificacion", nombre)
		}
		pesos[tipo] = n
		total += n
	}
	if total == 0 {
		return nil, errors.New("la mezcla suma cero")
	}
	return pesos, nil
}

// describirTasa pone la tasa en palabras para el encabezado
func describirTasa(tasa int) string {
	if tasa == 0 {
		return "maxima velocidad"
	}
	return fmt.Sprintf("%d solicitudes por minuto por cliente", tasa)
}

// crearCSV crea el archivo y su carpeta si no existe
func crearCSV(ruta string) (*os.File, error) {
	if carpeta := filepath.Dir(ruta); carpeta != "." {
		if err := os.MkdirAll(carpeta, 0o755); err != nil {
			return nil, err
		}
	}
	return os.Create(ruta)
}

// registro convierte una fila en los campos del csv
func (f fila) registro() []string {
	return []string{
		f.etiqueta, strconv.Itoa(f.escenario), f.cliente, f.perfil, string(f.operacion), f.idSolicitud,
		f.inicio.Format(time.RFC3339Nano), ms(f.desdeInicio), ms(f.duracion),
		strconv.FormatBool(f.exito), strconv.FormatBool(f.errorRed), f.instancia, strconv.FormatBool(f.calentamiento), f.mensaje,
	}
}

// ms escribe una duracion en milisegundos con tres decimales
func ms(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 3, 64)
}

// metrica es el resumen de una operacion en una prueba
type metrica struct {
	etiqueta string
	escenario int
	operacion model.TipoSolicitud
	total int // respuestas recibidas fuera del calentamiento
	exitos int // de esas, cuantas se hicieron
	erroresRed int // sin respuesta despues de los reintentos
	promedio float64 // tiempo de respuesta en ms
	desviacion float64 // desviacion estandar en ms
	p95 float64 // percentil 95 en ms
	tpm float64 // respuestas por minuto
	tpmExitos float64 // operaciones hechas por minuto
}

// resumir saca las metricas por etiqueta, escenario y operacion. solo cuenta lo que esta fuera del
// calentamiento, y el tiempo de respuesta solo de las solicitudes que si recibieron respuesta
func resumir(filas []fila, efectivo time.Duration) []metrica {
	type clave struct {
		etiqueta string
		escenario int
		operacion model.TipoSolicitud
	}
	tiempos := map[clave][]float64{}
	cuentas := map[clave]*metrica{}
	for _, f := range filas {
		if f.calentamiento {
			continue
		}
		k := clave{f.etiqueta, f.escenario, f.operacion}
		m := cuentas[k]
		if m == nil {
			m = &metrica{etiqueta: f.etiqueta, escenario: f.escenario, operacion: f.operacion}
			cuentas[k] = m
		}
		if f.errorRed {
			m.erroresRed++
			continue
		}
		m.total++
		if f.exito {
			m.exitos++
		}
		tiempos[k] = append(tiempos[k], float64(f.duracion)/float64(time.Millisecond))
	}
	minutos := efectivo.Minutes()
	var lista []metrica
	for k, m := range cuentas {
		m.promedio, m.desviacion, m.p95 = estadisticas(tiempos[k])
		if minutos > 0 {
			m.tpm = float64(m.total) / minutos
			m.tpmExitos = float64(m.exitos) / minutos
		}
		lista = append(lista, *m)
	}
	sort.Slice(lista, func(i, j int) bool {
		a, b := lista[i], lista[j]
		if a.etiqueta != b.etiqueta {
			return a.etiqueta < b.etiqueta
		}
		if a.escenario != b.escenario {
			return a.escenario < b.escenario
		}
		return a.operacion < b.operacion
	})
	return lista
}

// estadisticas calcula promedio, desviacion estandar muestral y percentil 95
func estadisticas(datos []float64) (promedio, desviacion, p95 float64) {
	n := len(datos)
	if n == 0 {
		return 0, 0, 0
	}
	suma := 0.0
	for _, x := range datos {
		suma += x
	}
	promedio = suma / float64(n)
	if n > 1 {
		cuadrados := 0.0
		for _, x := range datos {
			cuadrados += (x - promedio) * (x - promedio)
		}
		desviacion = math.Sqrt(cuadrados / float64(n-1))
	}
	ordenados := append([]float64(nil), datos...)
	sort.Float64s(ordenados)
	// percentil 95 por el metodo del rango mas cercano
	p95 = ordenados[int(math.Ceil(0.95*float64(n)))-1]
	return promedio, desviacion, p95
}

// imprimirResumen muestra las metricas como tabla
func imprimirResumen(lista []metrica) {
	fmt.Printf("%-10s %-4s %-13s %7s %7s %6s %11s %11s %9s %8s %8s\n", "etiqueta", "esc", "operacion", "total", "exitos", "red", "prom (ms)", "desv (ms)", "p95 (ms)", "TPM", "TPM ok")
	for _, m := range lista {
		fmt.Printf("%-10s %-4d %-13s %7d %7d %6d %11.2f %11.2f %9.2f %8.1f %8.1f\n", m.etiqueta, m.escenario, m.operacion, m.total, m.exitos, m.erroresRed, m.promedio, m.desviacion, m.p95, m.tpm, m.tpmExitos)
	}
}

// analizar es el subcomando que lee los csv de carga y saca las metricas de la tabla 1
func analizar(argumentos []string) error {
	fs := flag.NewFlagSet("analizar", flag.ExitOnError)
	salida := fs.String("salida", "metrics/resumen.csv", "csv donde se guarda el resumen, sirve para las graficas")
	fs.Parse(argumentos)
	archivos := fs.Args()
	if len(archivos) == 0 {
		archivos, _ = filepath.Glob(filepath.Join("metrics", "*-e*.csv"))
	}
	if len(archivos) == 0 {
		return errors.New("no hay csv para analizar, paselos como argumentos o deje los de carga en metrics/")
	}
	var lista []metrica
	for _, ruta := range archivos {
		filas, efectivo, err := leerCSV(ruta)
		if err != nil {
			return fmt.Errorf("%s: %w", ruta, err)
		}
		lista = append(lista, resumir(filas, efectivo)...)
	}
	imprimirResumen(lista)

	archivo, err := crearCSV(*salida)
	if err != nil {
		return err
	}
	defer archivo.Close()
	w := csv.NewWriter(archivo)
	w.Write([]string{"etiqueta", "escenario", "operacion", "total", "exitos", "errores_red", "promedio_ms", "desviacion_ms", "p95_ms", "tpm", "tpm_exitos"})
	for _, m := range lista {
		w.Write([]string{m.etiqueta, strconv.Itoa(m.escenario), string(m.operacion), strconv.Itoa(m.total), strconv.Itoa(m.exitos), strconv.Itoa(m.erroresRed),
			f2(m.promedio), f2(m.desviacion), f2(m.p95), f2(m.tpm), f2(m.tpmExitos)})
	}
	w.Flush()
	fmt.Printf("\nresumen guardado en %s\n", *salida)
	return w.Error()
}

// leerCSV lee un csv de carga y calcula su tiempo efectivo
func leerCSV(ruta string) ([]fila, time.Duration, error) {
	archivo, err := os.Open(ruta)
	if err != nil {
		return nil, 0, err
	}
	defer archivo.Close()
	registros, err := csv.NewReader(archivo).ReadAll()
	if err != nil {
		return nil, 0, err
	}
	var filas []fila
	for i, r := range registros {
		if i == 0 || len(r) < 14 {
			continue
		}
		esc, _ := strconv.Atoi(r[1])
		desde, _ := strconv.ParseFloat(r[7], 64)
		dur, _ := strconv.ParseFloat(r[8], 64)
		f := fila{etiqueta: r[0], escenario: esc, cliente: r[2], perfil: r[3], operacion: model.TipoSolicitud(r[4]), idSolicitud: r[5],
			desdeInicio: time.Duration(desde * float64(time.Millisecond)), duracion: time.Duration(dur * float64(time.Millisecond)),
			exito: r[9] == "true", errorRed: r[10] == "true", instancia: r[11], calentamiento: r[12] == "true", mensaje: r[13]}
		filas = append(filas, f)
	}
	return filas, efectivoDe(filas), nil
}

// efectivoDe es el tiempo que se mide, desde la primera solicitud fuera del calentamiento hasta que
// termina la ultima. con eso se calculan las transacciones por minuto
func efectivoDe(filas []fila) time.Duration {
	var primera, ultima time.Duration = -1, 0
	for _, f := range filas {
		if f.calentamiento {
			continue
		}
		if primera < 0 || f.desdeInicio < primera {
			primera = f.desdeInicio
		}
		if fin := f.desdeInicio + f.duracion; fin > ultima {
			ultima = fin
		}
	}
	if primera < 0 {
		return 0
	}
	return ultima - primera
}

// f2 escribe un numero con dos decimales
func f2(x float64) string {
	return strconv.FormatFloat(x, 'f', 2, 64)
}
