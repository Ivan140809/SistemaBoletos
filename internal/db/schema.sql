CREATE SCHEMA IF NOT EXISTS srb;

CREATE TABLE IF NOT EXISTS srb.events(
id_evento text PRIMARY KEY,
nombre_evento text NOT NULL,
posicion integer NOT NULL
);

CREATE TABLE IF NOT EXISTS srb.ocurrencias(
id_evento text NOT NULL REFERENCES srb.events(id_evento),
id_ocurrencia integer NOT NULL CHECK (id_ocurrencia >=1),
fecha date NOT NULL,
capacidad_total integer NOT NULL CHECK (capacidad_total >=1),
asientos_disponibles integer NOT NULL,
asientos_vendidos integer NOT NULL DEFAULT 0,
PRIMARY KEY (id_evento, id_ocurrencia),
CONSTRAINT asientos_no_negativos CHECK (asientos_disponibles >= 0 AND asientos_vendidos >= 0),
CONSTRAINT asientos_validos CHECK (asientos_disponibles + asientos_vendidos = capacidad_total)
);

CREATE SEQUENCE IF NOT EXISTS srb.reservas_seq;

CREATE TABLE IF NOT EXISTS srb.reservas(
id_reserva text PRIMARY KEY,
id_cliente text NOT NULL,
id_evento text NOT NULL,
id_ocurrencia integer NOT NULL,
cantidad_asientos  integer NOT NULL CHECK (cantidad_asientos > 0),
estado text NOT NULL DEFAULT 'ACTIVA' CHECK (estado IN ('ACTIVA', 'CANCELADA')),
creada_en timestamptz NOT NULL DEFAULT now(),
FOREIGN KEY (id_evento, id_ocurrencia) REFERENCES srb.ocurrencias (id_evento, id_ocurrencia)
);

CREATE INDEX IF NOT EXISTS reservas_por_cliente ON srb.reservas (id_cliente);

CREATE TABLE IF NOT EXISTS srb.solicitudes_procesadas(
id_solicitud text PRIMARY KEY,
operacion text NOT NULL,
respuesta jsonb,
procesada_en timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW srb.vista_ocupacion AS
SELECT
e.posicion, e.id_evento, e.nombre_evento, o.id_ocurrencia, o.fecha, o.capacidad_total, o.asientos_disponibles, o.asientos_vendidos, round(100.0 * o.asientos_vendidos / o.capacidad_total, 1) AS porcentaje_ocupacion
FROM srb.events e
JOIN srb.ocurrencias o ON e.id_evento = o.id_evento
ORDER BY e.posicion, o.id_ocurrencia;

CREATE OR REPLACE FUNCTION srb.rechazo(p_id_solicitud text, p_mensaje text)
RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
SELECT jsonb_build_object('id_solicitud', p_id_solicitud, 'exito', false, 'mensaje', p_mensaje);
$$;

CREATE OR REPLACE FUNCTION srb.reserva_json(r srb.reservas)
RETURNS jsonb
LANGUAGE sql STABLE AS $$
SELECT jsonb_build_object(
'id_reserva', r.id_reserva,
'id_cliente', r.id_cliente,
'id_evento', r.id_evento,
'id_ocurrencia', r.id_ocurrencia,
'cantidad_asientos', r.cantidad_asientos,
'estado', r.estado,
'creada', r.creada_en);
$$;

CREATE OR REPLACE FUNCTION srb.crear_reserva(
p_id_solicitud text,
p_id_cliente text,
p_id_evento text,
p_id_ocurrencia integer,
p_cantidad integer)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
v_respuesta jsonb;
v_disponibles integer;
v_numero bigint;
v_reserva srb.reservas;
BEGIN
IF p_cantidad IS NULL OR p_cantidad < 1 THEN
    RETURN srb.rechazo(p_id_solicitud, 'la cantidad de asientos debe ser mayor que cero');
END IF;

INSERT INTO srb.solicitudes_procesadas (id_solicitud, operacion)
VALUES (p_id_solicitud, 'crear_reserva')
ON CONFLICT (id_solicitud) DO NOTHING;
IF NOT FOUND THEN
SELECT respuesta INTO v_respuesta FROM srb.solicitudes_procesadas WHERE id_solicitud = p_id_solicitud;
RETURN v_respuesta;
END IF;

UPDATE srb.ocurrencias
    SET asientos_disponibles = asientos_disponibles - p_cantidad,
        asientos_vendidos    = asientos_vendidos + p_cantidad
    WHERE id_evento = p_id_evento
      AND id_ocurrencia = p_id_ocurrencia
      AND asientos_disponibles >= p_cantidad
    RETURNING asientos_disponibles INTO v_disponibles;

IF NOT FOUND THEN
        SELECT asientos_disponibles INTO v_disponibles
          FROM srb.ocurrencias
         WHERE id_evento = p_id_evento AND id_ocurrencia = p_id_ocurrencia;
        IF v_disponibles IS NULL THEN
            v_respuesta := srb.rechazo(p_id_solicitud, format('la ocurrencia %s/%s no existe', p_id_evento, p_id_ocurrencia));
        ELSE
            v_respuesta := srb.rechazo(p_id_solicitud, format('no hay asientos suficientes en %s/%s: pidió %s, quedan %s', p_id_evento, p_id_ocurrencia, p_cantidad, v_disponibles));
        END IF;
    ELSE
        v_numero := nextval('srb.reservas_seq');
        INSERT INTO srb.reservas (id_reserva, id_cliente, id_evento, id_ocurrencia, cantidad_asientos)
        VALUES ('R' || lpad(v_numero::text, greatest(4, length(v_numero::text)), '0'), p_id_cliente, p_id_evento, p_id_ocurrencia, p_cantidad)
        RETURNING * INTO v_reserva;
        v_respuesta := jsonb_build_object(
            'id_solicitud', p_id_solicitud,
            'exito', true,
            'mensaje', format('reserva %s confirmada: %s asientos en %s/%s, quedan %s', v_reserva.id_reserva, p_cantidad, p_id_evento, p_id_ocurrencia, v_disponibles),
            'reserva', srb.reserva_json(v_reserva));
    END IF;

    UPDATE srb.solicitudes_procesadas SET respuesta = v_respuesta WHERE id_solicitud = p_id_solicitud;
    RETURN v_respuesta;
END;
$$;
