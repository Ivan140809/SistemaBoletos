package db

// Este archivo produce el ESTADO INICIAL de la base: o lo lee de un archivo
// JSON (LeerEventos) o lo genera automáticamente (GenerarEventos). El resultado,
// una lista de eventos, se le pasa a BaseDatos.Reiniciar para cargarlo.
//
// Importante para la sustentación: el enunciado dice que el profesor entrega el
// caso de prueba (cuántos eventos y qué operaciones). Para eso se escribe un
// JSON con el mismo formato que data/test_case.json y se arranca persistencia con
//   ./scripts/machine3_storage.sh -inicial ruta/del/caso.json

import (
	"encoding/json"
	"fmt"
	"os"
	"sistemaboletos/internal/model"
	"time"
)

// LeerEventos lee un archivo JSON con el formato {"eventos": [ ... ]} y lo
// convierte en una lista de eventos.
func LeerEventos(ruta string) ([]model.Evento, error) {
	datos, err := os.ReadFile(ruta) // lee el archivo completo como bytes
	if err != nil {
		return nil, err
	}
	// Struct "anónimo" (sin nombre) definido en el momento, solo para decodificar
	// el JSON: el archivo tiene un objeto con una clave "eventos" que contiene la lista.
	var archivo struct {
		Eventos []model.Evento `json:"eventos"`
	}
	if err := json.Unmarshal(datos, &archivo); err != nil {
		return nil, fmt.Errorf("el archivo %s no es un json valido: %w", ruta, err)
	}
	return archivo.Eventos, nil
}

// GenerarEventos crea eventos de prueba SIN archivo (opción -generar de
// persistencia). Con los valores por defecto produce exactamente el escenario
// del enunciado: 50 eventos en un mes, 20 de ellos con dos fechas y el resto
// con una, y 100 asientos por fecha.
func GenerarEventos(mes string, cantidad, dobles, asientos int) ([]model.Evento, error) {
	inicioMes, err := time.Parse("2006-01", mes)
	if err != nil {
		return nil, fmt.Errorf("el mes %q no sirve, escribalo como 2026-11", mes)
	}
	// Nombres de tipos de evento, repartidos en rotación.
	tipos := []string{"Conciertos", "Obras de teatro", "Partido de futbol", "Carnaval", "Torneos"}
	var eventos []model.Evento
	// Ciclo "for" clásico: for inicio; condicion; paso { cuerpo }.
	for i := 0; i < cantidad; i++ {
		// i%len(tipos) es el resto de la división: recorre 0,1,2,3,4,0,1,2...
		evento := model.Evento{IdEvento: fmt.Sprintf("E%d", i+1), Nombre: fmt.Sprintf("%s %d", tipos[i%len(tipos)], i+1)}
		ocurrencias := 1
		// Los primeros "dobles" eventos (E1 a E20) tienen dos fechas.
		if i < dobles {
			ocurrencias = 2
		}
		for o := 0; o < ocurrencias; o++ {
			// Fecha = día 1 del mes + (i mod 14) días + 14 días si es la segunda
			// ocurrencia. Así la primera cae entre el día 1 y el 14, la segunda entre el 15 y el 28.
			fecha := inicioMes.AddDate(0, 0, i%14+o*14)
			evento.Ocurrencias = append(evento.Ocurrencias, model.Ocurrencia{
				IdOcurrencia:        o + 1,
				Fecha:               fecha.Format("2006-01-02"),
				CapacidadTotal:      asientos,
				AsientosDisponibles: asientos, // al inicio todos los asientos están libres
			})
		}
		eventos = append(eventos, evento)
	}
	return eventos, nil
}
