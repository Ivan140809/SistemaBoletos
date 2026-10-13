package db

// pruebas automaticas del paquete db. en go, un archivo que termina en _test.go
// tiene pruebas y se corren con:   go test ./...
// cada funcion que empieza con "test" y recibe (t *testing.t) es una prueba, y
// con t.fatal / t.error se marca que fallo.
//
// estas pruebas no corren en el sistema real, son para el equipo. las que
// necesitan base de datos se saltan solas si no esta definida la variable
// srb_test_database_url (que tiene que apuntar a una bd de pruebas, porque se
// borran sus datos). mirar .env.example.

import (
	"context"
	"fmt"
	"os"
	"sistemaboletos/internal/model"
	"strings"
	"sync"
	"testing"
)

// prueba 1 (no necesita bd): el generador produce el escenario del enunciado, o
// sea 50 eventos y 70 ocurrencias (20 eventos x 2 fechas + 30 x 1 fecha), todas
// en noviembre y con 100 asientos libres.
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

// basedeprueba es un ayudante (no es una prueba): conecta a la bd de pruebas y la
// deja con 2 eventos nuevos de 100 asientos.
func baseDePrueba(t *testing.T) *BaseDatos {
	dsn := os.Getenv("SRB_TEST_DATABASE_URL")
	if dsn == "" {
		// skip es "saltar esta prueba", no cuenta como fallo.
		t.Skip("defina SRB_TEST_DATABASE_URL con una BD de pruebas (sus datos se borran)")
	}
	base, err := Conectar(dsn)
	if err != nil {
		t.Fatal(err)
	}
	// cleanup registra algo que corre al terminar la prueba (aqui cierra la base).
	t.Cleanup(base.Cerrar)
	eventos, _ := GenerarEventos("2026-11", 2, 0, 100)
	if err := base.Reiniciar(context.Background(), eventos); err != nil {
		t.Fatal(err)
	}
	return base
}

// pedirreserva arma una solicitud de reserva (ocurrencia 1) para las pruebas.
func pedirReserva(id, cliente, evento string, asientos int) model.Solicitud {
	return model.Solicitud{IdSolicitud: id, IdCliente: cliente, Tipo: model.TipoReserva, IdEvento: evento, IdOcurrencia: 1, CantidadAsientos: asientos}
}

// prueba 2: concurrencia. lanza 20 "clientes" a la vez (cada uno es una
// goroutine) pidiendo 7 asientos del mismo evento de 100. entre los 20 piden 140,
// asi que solo se pueden aprobar 14 reservas (14 x 7 = 98) y tienen que quedar 2
// libres: nunca se puede vender mas de lo que hay (no hay sobreventa).
// demuestra que la funcion sql crear_reserva es atomica.
func TestReservasConcurrentesNoSobrevenden(t *testing.T) {
	base := baseDePrueba(t)
	ctx := context.Background()
	// waitgroup es un contador para esperar a que terminen todas las goroutines.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1) // una goroutine mas por esperar
		// "go func(...) {...}(i)" lanza esa funcion en una goroutine, un hilo
		// ligero de go que corre en paralelo con el resto.
		go func(i int) {
			defer wg.Done() // al terminar, avisa que ya acabo
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

// prueba 3: idempotencia. se manda dos veces la misma solicitud (mismo
// idsolicitud "s1"). la segunda no debe descontar asientos otra vez y tiene que
// devolver la misma reserva que la primera. es lo que protege de los reintentos
// duplicados (por ejemplo cuando se repite una peticion despues de una falla).
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
