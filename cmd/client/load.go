package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
	"gonum.org/v1/gonum/stat"
	"sistemaboletos/internal/communication"
	"sistemaboletos/internal/model"
)

type ConfigCarga struct {
	Mes string `json:"mes"`
	DuracionSegundos int `json:"duracion_segundos"`
	CalentamientoSegundos int `json:"calentamiento_segundos"`
	Semilla int64 `json:"semilla"`
	MaxAsientosReserva int `json:"max_asientos_reserva"`
	ArchivoCSV  string `json:"archivo_csv"`
	Grupos []GrupoClientes `json:"grupos"`
}
type GrupoClientes struct {
	Nombre string `json:"nombre"`
	Cantidad int `json:"cantidad"`
	SolicitudesPorMinuto int            `json:"solicitudes_por_minuto"`
	Porcentajes map[string]int `json:"porcentajes"`
}

type medicion struct {
	idSolicitud string
	cliente string
	operacion  model.TipoSolicitud
	inicio time.Time
	duracion time.Duration
	exito bool
	mensaje string
	err error
}

func carga(argumentos []string) error {
	fs := flag.NewFlagSet("carga", flag.ExitOnError)
	gt := fs.String("gt", entorno("SRB_GT", "tcp://localhost:5555"), "endpoint del Gestor de Transacciones")
	rutaConfig := fs.String("config", "tests/scenario1.json", "archivo JSON con el escenario de carga")
	timeout := fs.Duration("timeout", 5*time.Second, "tiempo máximo de espera por cada respuesta")
	fs.Parse(argumentos)

	cfg, err := leerConfig(*rutaConfig)
	if err != nil {
		return err
	}
	eventos, err := descubrirEventos(*gt, cfg.Mes, *timeout) 
	if err != nil {
		return err
	}

	var ids []string
	var grupos []GrupoClientes
	for _, grupo := range cfg.Grupos {
		for i := 0; i < grupo.Cantidad; i++ {
			ids = append(ids, fmt.Sprintf("%s-%d", grupo.Nombre, i+1))
			grupos = append(grupos, grupo)
		}
	}

	mediciones := make([][]medicion, len(ids))
	salida := make(chan struct{})
	var terminados sync.WaitGroup
	for i := range ids {
		terminados.Add(1)
		go func(i int) {
			defer terminados.Done()
			aleatorio := rand.New(rand.NewSource(cfg.Semilla + int64(i))) // semilla fija por cliente: misma carga en cada corrida
			mediciones[i] = ejecutarCliente(*gt, *timeout, cfg, grupos[i], ids[i], aleatorio, eventos, salida)
		}(i)
	}
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("Escenario %s: %d clientes durante %ds (calentamiento %ds)\n", *rutaConfig, len(ids), cfg.DuracionSegundos, cfg.CalentamientoSegundos)
	inicio := time.Now()
	close(salida) 
	terminados.Wait()
	var todas []medicion
	for _, m := range mediciones {
		todas = append(todas, m...)
	}
	if err := escribirCSV(cfg.ArchivoCSV, todas); err != nil {
		return err
	}
	resumen(todas, inicio, cfg)
	fmt.Printf("Mediciones guardadas en %s\n", cfg.ArchivoCSV)
	return nil
}

func ejecutarCliente(gt string, timeout time.Duration, cfg ConfigCarga, grupo GrupoClientes, id string, aleatorio *rand.Rand, eventos []model.Evento, salida <-chan struct{}) []medicion {
	cliente, err := communication.NuevoClienteReq(gt, timeout) // socket REQ propio de este cliente
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] %v\n", id, err)
		return nil
	}
defer cliente.Cerrar()
<-salida
ticker := time.NewTicker(time.Minute / time.Duration(grupo.SolicitudesPorMinuto)) // la tasa no depende de la velocidad de la máquina
defer ticker.Stop()
duracion := time.Duration(cfg.DuracionSegundos) * time.Second
inicio := time.Now()
var mediciones []medicion
	for time.Since(inicio) < duracion {
		s := generarSolicitud(aleatorio, id, cfg, grupo, eventos)
		m := medicion{idSolicitud: s.IdSolicitud, cliente: id, operacion: s.Tipo, inicio: time.Now()}
		var resp model.Respuesta
		m.err = cliente.Pedir(s, &resp)
		m.duracion = time.Since(m.inicio)
		m.exito, m.mensaje = resp.Exito, resp.Mensaje
		if m.err != nil {
			m.mensaje = m.err.Error()
		}
		mediciones = append(mediciones, m) 
		<-ticker.C
	}
	return mediciones
}
func generarSolicitud(aleatorio *rand.Rand, id string, cfg ConfigCarga, grupo GrupoClientes, eventos []model.Evento) model.Solicitud {
	s := model.Solicitud{IdSolicitud: model.NuevoID(id), IdCliente: id, Tipo: elegirTipo(aleatorio, grupo.Porcentajes)}
	switch s.Tipo {
	case model.TipoConsulta:
		s.Mes = cfg.Mes
	case model.TipoReserva:
		evento := eventos[aleatorio.Intn(len(eventos))]
		ocurrencia := evento.Ocurrencias[aleatorio.Intn(len(evento.Ocurrencias))]
		s.IdEvento, s.IdOcurrencia = evento.IdEvento, ocurrencia.IdOcurrencia
		s.CantidadAsientos = 1 + aleatorio.Intn(cfg.MaxAsientosReserva)
	}
	return s
}

func elegirTipo(aleatorio *rand.Rand, porcentajes map[string]int) model.TipoSolicitud {
	valor := aleatorio.Intn(100)
	acumulado := 0
	for _, tipo := range []model.TipoSolicitud{model.TipoConsulta, model.TipoReserva} { // orden fijo para que la semilla sea reproducible
		acumulado += porcentajes[string(tipo)]
		if valor < acumulado {
			return tipo
		}
	}
	return model.TipoConsulta
}

func leerConfig(ruta string) (ConfigCarga, error) {
	var cfg ConfigCarga
	datos, err := os.ReadFile(ruta)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(datos, &cfg); err != nil {
		return cfg, fmt.Errorf("%s no es un JSON válido: %w", ruta, err)
	}
	if cfg.DuracionSegundos <= cfg.CalentamientoSegundos {
		return cfg, errors.New("la duración debe ser mayor que el calentamiento")
	}
	if cfg.MaxAsientosReserva < 1 {
		cfg.MaxAsientosReserva = 1
	}
	if cfg.ArchivoCSV == "" {
		cfg.ArchivoCSV = "metrics/carga.csv"
	}
	for _, grupo := range cfg.Grupos {
		if grupo.SolicitudesPorMinuto < 20 {
			return cfg, fmt.Errorf("grupo %s: el enunciado exige al menos 20 solicitudes por minuto por cliente", grupo.Nombre)
		}
		suma := 0
		for tipo, porcentaje := range grupo.Porcentajes {
			if tipo != string(model.TipoConsulta) && tipo != string(model.TipoReserva) {
				return cfg, fmt.Errorf("grupo %s: la operación %s no está disponible en la arquitectura base", grupo.Nombre, tipo)
			}
			suma += porcentaje
		}
		if suma != 100 {
			return cfg, fmt.Errorf("grupo %s: los porcentajes suman %d, deben sumar 100", grupo.Nombre, suma)
		}
	}
	return cfg, nil
}

func descubrirEventos(gt, mes string, timeout time.Duration) ([]model.Evento, error) {
	cliente, err := communication.NuevoClienteReq(gt, timeout)
	if err != nil {
		return nil, err
	}
	defer cliente.Cerrar()
	var resp model.Respuesta
	s := model.Solicitud{IdSolicitud: model.NuevoID("generador"), IdCliente: "generador", Tipo: model.TipoConsulta, Mes: mes}
	if err := cliente.Pedir(s, &resp); err != nil {
		return nil, err
	}
	if !resp.Exito || len(resp.Eventos) == 0 {
		return nil, fmt.Errorf("no hay eventos en %s: %s", mes, resp.Mensaje)
	}
	return resp.Eventos, nil
}

func escribirCSV(ruta string, mediciones []medicion) error {
	if err := os.MkdirAll(filepath.Dir(ruta), 0o755); err != nil {
		return err
	}
	archivo, err := os.Create(ruta)
	if err != nil {
		return err
	}
	defer archivo.Close()
	escritor := csv.NewWriter(archivo)
	escritor.Write([]string{"id_solicitud", "cliente", "operacion", "inicio_unix_ms", "duracion_ms", "exito", "mensaje"})
	for _, m := range mediciones {
		escritor.Write([]string{m.idSolicitud,m.cliente,string(m.operacion),strconv.FormatInt(m.inicio.UnixMilli(), 10), strconv.FormatFloat(float64(m.duracion.Microseconds())/1000, 'f', 3, 64), strconv.FormatBool(m.exito),m.mensaje,
		})
	}
	escritor.Flush()
	return escritor.Error()
}

func resumen(mediciones []medicion, inicio time.Time, cfg ConfigCarga) {
	calentamiento := time.Duration(cfg.CalentamientoSegundos) * time.Second
	ventana := time.Duration(cfg.DuracionSegundos)*time.Second - calentamiento
	tiempos := map[model.TipoSolicitud][]float64{}
	exitos := map[model.TipoSolicitud]int{}
	sinRespuesta := 0
	for _, m := range mediciones {
		if m.inicio.Sub(inicio)<calentamiento {
			continue 
		}
		if m.err != nil {
			sinRespuesta++
			continue
		}
		tiempos[m.operacion] = append(tiempos[m.operacion], float64(m.duracion.Microseconds())/1000)
		if m.exito {
			exitos[m.operacion]++
		}
	}
	fmt.Printf("\n%-10s %7s %8s %13s %13s %10s %9s\n", "Operacion", "Total", "Exito", "PROMEDIO(ms)", "DESV.EST(ms)", "P95(ms)", "TPM")
	for _, tipo := range []model.TipoSolicitud{model.TipoConsulta, model.TipoReserva} {
		datos := tiempos[tipo]
		if len(datos) == 0 {
			continue
		}
		sort.Float64s(datos) // stat.Quantile necesita los datos ordenados
		media, desviacion := stat.MeanStdDev(datos, nil)
		p95 := stat.Quantile(0.95, stat.Empirical, datos, nil)
		tpm := float64(len(datos)) / ventana.Minutes()
		fmt.Printf("%-10s %7d %8d %13.3f %13.3f %10.3f %9.1f\n", tipo, len(datos), exitos[tipo], media, desviacion, p95, tpm)
	}
	fmt.Printf("Solicitudes sin respuesta (timeout): %d\n", sinRespuesta)
}
