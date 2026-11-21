// pruebas automaticas del paquete db que se corren con go test y no forman parte del sistema real.
// las que usan base se saltan solas si no esta SRB_TEST_DATABASE_URL, que debe ser una base
// de pruebas porque sus datos se borran, y demuestran que no hay sobreventa ni reservas duplicadas.
package db

import (
	"context"
	"fmt"
	"os"
	"sistemaboletos/internal/model"
	"strings"
	"sync"
	"testing"
)

// prueba 1 sin base, el generador debe dar 50 eventos y 70 fechas en noviembre con 100 asientos
func TestGenerarEventos(t *testing.T) {
	// aqui se crean los eventos de prueba
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

// baseDePrueba conecta a la base de pruebas y la deja con 2 eventos de 100 asientos
func baseDePrueba(t *testing.T) *BaseDatos {
	dsn := os.Getenv("SRB_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("defina SRB_TEST_DATABASE_URL con una BD de pruebas (sus datos se borran)")
	}
	// aqui se crea la conexion con un pool de 4 conexiones
	base, err := Conectar(dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(base.Cerrar)
	// aqui se crean 2 eventos nuevos
	eventos, _ := GenerarEventos("2026-11", 2, 0, 100)
	if err := base.Reiniciar(context.Background(), eventos); err != nil {
		t.Fatal(err)
	}
	return base
}

// pedirReserva arma una solicitud de reserva para la fecha 1
func pedirReserva(id, cliente, evento string, asientos int) model.Solicitud {
	// aqui se crea la solicitud
	return model.Solicitud{IdSolicitud: id, IdCliente: cliente, Tipo: model.TipoReserva, IdEvento: evento, IdOcurrencia: 1, CantidadAsientos: asientos}
}

// prueba 2 de concurrencia, 20 clientes a la vez piden 7 asientos de 100 y solo caben 14 reservas.
// deben quedar 2 libres, asi se demuestra que crear_reserva es atomica y no hay sobreventa
func TestReservasConcurrentesNoSobrevenden(t *testing.T) {
	base := baseDePrueba(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		// aqui se crea la goroutine de un cliente
		go func(i int) {
			defer wg.Done()
			if _, err := base.CrearReserva(ctx, pedirReserva(fmt.Sprintf("S%d", i), fmt.Sprintf("C%d", i), "E2", 7)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if libres, _ := base.Disponibilidad(ctx, "E2", 1); libres != 2 {
		t.Fatalf("se esperaban 2 asientos libres, hay %d", libres)
	}
}

// prueba 3 de idempotencia, la misma solicitud dos veces no descuenta asientos otra vez.
// devuelve la misma reserva y asi protege de los reintentos duplicados despues de una falla
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
