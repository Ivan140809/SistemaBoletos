package db

// PRUEBAS AUTOMÁTICAS del paquete db. En Go, un archivo que termina en _test.go
// contiene pruebas; se ejecutan con:   go test ./...
// Cada función que empieza con "Test" y recibe (t *testing.T) es una prueba.
// Con t.Fatal / t.Error se marca que falló.
//
// Estas pruebas NO se ejecutan en el sistema real: son para el equipo.
// Las que necesitan base de datos se saltan solas si no se define la variable
// de entorno SRB_TEST_DATABASE_URL (apuntando a una BD de pruebas, porque se
// BORRAN sus datos). Ver .env.example.

import (
	"context"
	"fmt"
	"os"
	"sistemaboletos/internal/model"
	"strings"
	"sync"
	"testing"
)

// Prueba 1 (no necesita BD): el generador produce el escenario del enunciado,
// es decir 50 eventos y 70 ocurrencias (20 eventos x 2 fechas + 30 x 1 fecha),
// todas en noviembre y con 100 asientos libres.
func TestGenerarEventos(t *testing.T) {
	eventos, err := GenerarEventos("2026-11", 50, 20, 100)
	if err != nil {
		t.Fatal(err)
	}
	ocurrencias := 0
	for _, evento := range eventos {
		for _, oc := range evento.Ocurrencias {
			ocurrencias++
			if !strings.HasPrefix(oc.Fecha, "2026-11") || oc.AsientosDisponibles != 100 {
				t.Fatalf("ocurrencia rara en %s: %+v", evento.IdEvento, oc)
			}
		}
	}
	if len(eventos) != 50 || ocurrencias != 70 {
		t.Fatalf("se esperaban 50 eventos y 70 ocurrencias, hay %d y %d", len(eventos), ocurrencias)
	}
}

// baseDePrueba es un ayudante (no es una prueba): conecta a la BD de pruebas
// y la deja con 2 eventos nuevos de 100 asientos.
func baseDePrueba(t *testing.T) *BaseDatos {
	dsn := os.Getenv("SRB_TEST_DATABASE_URL")
	if dsn == "" {
		// Skip = "saltar esta prueba" (no cuenta como fallo).
		t.Skip("defina SRB_TEST_DATABASE_URL con una BD de pruebas (sus datos se borran)")
	}
	base, err := Conectar(dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup registra algo que se ejecuta al terminar la prueba (cierra la base).
	t.Cleanup(base.Cerrar)
	eventos, _ := GenerarEventos("2026-11", 2, 0, 100)
	if err := base.Reiniciar(context.Background(), eventos); err != nil {
		t.Fatal(err)
	}
	return base
}

// pedirReserva arma una Solicitud de reserva (ocurrencia 1) para las pruebas.
func pedirReserva(id, cliente, evento string, asientos int) model.Solicitud {
	return model.Solicitud{IdSolicitud: id, IdCliente: cliente, Tipo: model.TipoReserva, IdEvento: evento, IdOcurrencia: 1, CantidadAsientos: asientos}
}

// Prueba 2: CONCURRENCIA. Lanza 20 "clientes" a la vez (cada uno es una
// goroutine) pidiendo 7 asientos del mismo evento de 100. Los 20 piden 140 en
// total, así que solo pueden aprobarse 14 reservas (14 x 7 = 98) y deben quedar
// 2 libres: nunca se puede vender más de lo que hay (no hay "sobreventa").
// Demuestra que la función SQL crear_reserva es atómica.
func TestReservasConcurrentesNoSobrevenden(t *testing.T) {
	base := baseDePrueba(t)
	ctx := context.Background()
	// WaitGroup = contador para esperar a que terminen todas las goroutines.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1) // una goroutine más por esperar
		// "go func(...) {...}(i)" lanza esa función en una goroutine: un hilo
		// ligero de Go que corre en paralelo con el resto.
		go func(i int) {
			defer wg.Done() // al terminar, avisa que ya acabó
			if _, err := base.CrearReserva(ctx, pedirReserva(fmt.Sprintf("S%d", i), fmt.Sprintf("C%d", i), "E2", 7)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait() // espera a que las 20 terminen
	if libres, _ := base.Disponibilidad(ctx, "E2", 1); libres != 2 { // 20 x 7 = 140 pedidos, solo caben 14 reservas (98)
		t.Fatalf("se esperaban 2 asientos libres, hay %d", libres)
	}
}

// Prueba 3: IDEMPOTENCIA. Se envía dos veces la MISMA solicitud (mismo
// IdSolicitud "S1"). La segunda no debe descontar asientos otra vez, y debe
// devolver la misma reserva que la primera. Es lo que protege de reintentos
// duplicados (por ejemplo cuando se repite una petición tras una falla).
func TestSolicitudRepetidaNoSeEjecutaDosVeces(t *testing.T) {
	base := baseDePrueba(t)
	ctx := context.Background()
	primera, err := base.CrearReserva(ctx, pedirReserva("S1", "C1", "E1", 5))
	if err != nil || !primera.Exito {
		t.Fatal(err, primera.Mensaje)
	}
	segunda, err := base.CrearReserva(ctx, pedirReserva("S1", "C1", "E1", 5))
	if err != nil {
		t.Fatal(err)
	}
	libres, _ := base.Disponibilidad(ctx, "E1", 1)
	if libres != 95 || segunda.Reserva == nil || primera.Reserva.IdReserva != segunda.Reserva.IdReserva {
		t.Fatalf("la solicitud repetida se ejecuto dos veces, quedan %d libres", libres)
	}
}
