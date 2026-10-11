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
				t.Fatalf("ocurrencia inválida en %s: %+v", evento.IdEvento, oc)
			}
		}
	}
	if len(eventos) != 50 || ocurrencias != 70 {
		t.Fatalf("se esperaban 50 eventos y 70 ocurrencias, hay %d y %d", len(eventos), ocurrencias)
	}
}

func baseDePrueba(t *testing.T) *BaseDatos {
	dsn := os.Getenv("SRB_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("defina SRB_TEST_DATABASE_URL con una BD de pruebas (sus datos se borran)")
	}
	base, err := Conectar(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(base.Cerrar)
	eventos, _ := GenerarEventos("2026-11", 2, 0, 100)
	if err := base.Reiniciar(context.Background(), eventos); err != nil {
		t.Fatal(err)
	}
	return base
}

func pedirReserva(id, cliente, evento string, asientos int) model.Solicitud {
	return model.Solicitud{IdSolicitud: id, IdCliente: cliente, Tipo: model.TipoReserva, IdEvento: evento, IdOcurrencia: 1, CantidadAsientos: asientos}
}

func TestReservasConcurrentesNoSobrevenden(t *testing.T) {
	base := baseDePrueba(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := base.CrearReserva(ctx, pedirReserva(fmt.Sprintf("S%d", i), fmt.Sprintf("C%d", i), "E2", 7)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if libres, _ := base.Disponibilidad(ctx, "E2", 1); libres != 2 { // 20 x 7 = 140 pedidos, solo caben 14 reservas (98)
		t.Fatalf("se esperaban 2 asientos libres, hay %d", libres)
	}
}

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
		t.Fatalf("la solicitud repetida se ejecutó dos veces: quedan %d libres", libres)
	}
}

func TestCancelacionLiberaAsientos(t *testing.T) {
	base := baseDePrueba(t)
	ctx := context.Background()
	creada, err := base.CrearReserva(ctx, pedirReserva("S1", "C1", "E1", 10))
	if err != nil || !creada.Exito {
		t.Fatal(err, creada.Mensaje)
	}
	cancelacion := model.Solicitud{IdSolicitud: "S2", IdCliente: "C1", Tipo: model.TipoCancelacion, IdReserva: creada.Reserva.IdReserva}
	if resp, err := base.CancelarReserva(ctx, cancelacion); err != nil || !resp.Exito {
		t.Fatal(err, resp.Mensaje)
	}
	if libres, _ := base.Disponibilidad(ctx, "E1", 1); libres != 100 {
		t.Fatalf("se esperaban 100 libres, hay %d", libres)
	}
	ajena := cancelacion
	ajena.IdSolicitud, ajena.IdCliente = "S3", "C2"
	if resp, _ := base.CancelarReserva(ctx, ajena); resp.Exito {
		t.Fatal("un cliente canceló una reserva ajena")
	}
}
