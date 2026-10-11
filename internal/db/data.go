package db

import (
	"encoding/json"
	"fmt"
	"os"
	"sistemaboletos/internal/model"
	"time"
)

func LeerEventos(ruta string) ([]model.Evento, error) {
	datos, err := os.ReadFile(ruta)
	if err != nil {
		return nil, err
	}
	var archivo struct {
		Eventos []model.Evento `json:"eventos"`
	}
	if err := json.Unmarshal(datos, &archivo); err != nil {
		return nil, fmt.Errorf("el archivo %s no es un json valido: %w", ruta, err)
	}
	return archivo.Eventos, nil
}

func GenerarEventos(mes string, cantidad, dobles, asientos int) ([]model.Evento, error) {
	inicioMes, err := time.Parse("2006-01", mes)
	if err != nil {
		return nil, fmt.Errorf("el mes %q no sirve, escribalo como 2026-11", mes)
	}
	tipos := []string{"Conciertos", "Obras de teatro", "Partido de futbol", "Carnaval", "Torneos"}
	var eventos []model.Evento
	for i := 0; i < cantidad; i++ {
		evento := model.Evento{IdEvento: fmt.Sprintf("E%d", i+1), Nombre: fmt.Sprintf("%s %d", tipos[i%len(tipos)], i+1)}
		ocurrencias := 1
		if i < dobles {
			ocurrencias = 2
		}
		for o := 0; o < ocurrencias; o++ {
			fecha := inicioMes.AddDate(0, 0, i%14+o*14)
			evento.Ocurrencias = append(evento.Ocurrencias, model.Ocurrencia{
				IdOcurrencia:        o + 1,
				Fecha:               fecha.Format("2006-01-02"),
				CapacidadTotal:      asientos,
				AsientosDisponibles: asientos,
			})
		}
		eventos = append(eventos, evento)
	}
	return eventos, nil
}
