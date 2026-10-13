package db

// este archivo produce el estado inicial de la base: o lo lee de un json
// (leereventos) o lo genera solo (generareventos). el resultado, una lista de
// eventos, se le pasa a basedatos.reiniciar para cargarlo.
//
// ojo para la sustentacion: el enunciado dice que el profesor entrega el caso de
// prueba (cuantos eventos y que operaciones). para eso se escribe un json con el
// mismo formato de data/test_case.json y se arranca persistencia con
//   ./scripts/machine3_storage.sh -inicial ruta/del/caso.json

import (
	"encoding/json"
	"fmt"
	"os"
	"sistemaboletos/internal/model"
	"time"
)

// leereventos lee un json con formato {"eventos": [ ... ]} y lo pasa a una lista de eventos.
func LeerEventos(ruta string) ([]model.Evento, error) {
	datos, err := os.ReadFile(ruta) // lee el archivo completo como bytes
	if err != nil {
		return nil, err
	}
	// struct anonimo (sin nombre), definido en el momento solo para decodificar el
	// json: el archivo es un objeto con una clave "eventos" que tiene la lista.
	var archivo struct {
		Eventos []model.Evento `json:"eventos"`
	}
	if err := json.Unmarshal(datos, &archivo); err != nil {
		return nil, fmt.Errorf("el archivo %s no es un json valido: %w", ruta, err)
	}
	return archivo.Eventos, nil
}

// generareventos crea eventos de prueba sin archivo (la opcion -generar de
// persistencia). con los valores por defecto sale justo el escenario del
// enunciado: 50 eventos en un mes, 20 con dos fechas y el resto con una, y 100
// asientos por fecha.
func GenerarEventos(mes string, cantidad, dobles, asientos int) ([]model.Evento, error) {
	inicioMes, err := time.Parse("2006-01", mes)
	if err != nil {
		return nil, fmt.Errorf("el mes %q no sirve, escribalo como 2026-11", mes)
	}
	// nombres de tipos de evento, repartidos en rotacion.
	tipos := []string{"Conciertos", "Obras de teatro", "Partido de futbol", "Carnaval", "Torneos"}
	var eventos []model.Evento
	// ciclo for clasico: for inicio; condicion; paso { cuerpo }.
	for i := 0; i < cantidad; i++ {
		// i%len(tipos) es el resto de la division, recorre 0,1,2,3,4,0,1,2...
		evento := model.Evento{IdEvento: fmt.Sprintf("E%d", i+1), Nombre: fmt.Sprintf("%s %d", tipos[i%len(tipos)], i+1)}
		ocurrencias := 1
		// los primeros "dobles" eventos (e1 a e20) tienen dos fechas.
		if i < dobles {
			ocurrencias = 2
		}
		for o := 0; o < ocurrencias; o++ {
			// fecha = dia 1 del mes + (i mod 14) dias + 14 dias si es la segunda
			// ocurrencia. asi la primera cae entre el dia 1 y el 14, y la segunda
			// entre el 15 y el 28.
			fecha := inicioMes.AddDate(0, 0, i%14+o*14)
			evento.Ocurrencias = append(evento.Ocurrencias, model.Ocurrencia{
				IdOcurrencia:        o + 1,
				Fecha:               fecha.Format("2006-01-02"),
				CapacidadTotal:      asientos,
				AsientosDisponibles: asientos, // al inicio todos los asientos estan libres
			})
		}
		eventos = append(eventos, evento)
	}
	return eventos, nil
}
