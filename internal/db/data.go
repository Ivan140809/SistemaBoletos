package db

// estado inicial de la base, se lee de un json o se genera solo y se carga con Reiniciar.
// para el caso del profesor se escribe un json con el formato de data/test_case.json
// y se arranca persistencia con -inicial y la ruta del archivo.

import (
	"encoding/json"
	"fmt"
	"os"
	"sistemaboletos/internal/model"
	"time"
)

// LeerEventos lee un json con una lista de eventos
func LeerEventos(ruta string) ([]model.Evento, error) {
	// lee el archivo completo
	datos, err := os.ReadFile(ruta)
	if err != nil {
		return nil, err
	}
	// aqui se crea la estructura que tiene la clave eventos del archivo
	var archivo struct {
		Eventos []model.Evento `json:"eventos"`
	}
	// pasa el json a la estructura
	if err := json.Unmarshal(datos, &archivo); err != nil {
		return nil, fmt.Errorf("el archivo %s no es un json valido: %w", ruta, err)
	}
	// devuelve la lista de eventos
	return archivo.Eventos, nil
}

// GenerarEventos crea eventos de prueba, por defecto 50 eventos, 20 con dos fechas y 100 asientos
func GenerarEventos(mes string, cantidad, dobles, asientos int) ([]model.Evento, error) {
	// convierte el mes al dia 1 de ese mes
	inicioMes, err := time.Parse("2006-01", mes)
	if err != nil {
		return nil, fmt.Errorf("el mes %q no sirve, escribalo como 2026-11", mes)
	}
	// tipos de evento que se van turnando
	tipos := []string{"Conciertos", "Obras de teatro", "Partido de futbol", "Carnaval", "Torneos"}
	// lista final de eventos
	var eventos []model.Evento
	// crea un evento por vuelta
	for i := 0; i < cantidad; i++ {
		// aqui se crea el evento con su id y nombre
		evento := model.Evento{IdEvento: fmt.Sprintf("E%d", i+1), Nombre: fmt.Sprintf("%s %d", tipos[i%len(tipos)], i+1)}
		// por defecto una fecha
		ocurrencias := 1
		// los primeros eventos tienen dos fechas
		if i < dobles {
			ocurrencias = 2
		}
		// crea cada fecha del evento
		for o := 0; o < ocurrencias; o++ {
			// la primera fecha cae entre el dia 1 y el 14 y la segunda entre el 15 y el 28
			fecha := inicioMes.AddDate(0, 0, i%14+o*14)
			// aqui se crea la fecha con todos los asientos libres
			evento.Ocurrencias = append(evento.Ocurrencias, model.Ocurrencia{
				IdOcurrencia:        o + 1,
				Fecha:               fecha.Format("2006-01-02"),
				CapacidadTotal:      asientos,
				AsientosDisponibles: asientos,
			})
		}
		// agrega el evento a la lista
		eventos = append(eventos, evento)
	}
	// devuelve todos los eventos
	return eventos, nil
}
