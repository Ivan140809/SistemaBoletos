-- esquema de la base, va dentro del ejecutable y se corre cada vez que arranca persistencia.
-- todo usa if not exists u or replace, asi que si ya existe no pasa nada y si falta se crea.
-- las tablas viven en el esquema srb, en supabase hay que cambiar el selector de public a srb.
-- un evento tiene varias ocurrencias, una ocurrencia tiene varias reservas y
-- solicitudes_procesadas evita repetir solicitudes.

-- aqui se crea el esquema srb
CREATE SCHEMA IF NOT EXISTS srb;

-- aqui se crea la tabla de eventos, posicion guarda el orden de carga
CREATE TABLE IF NOT EXISTS srb.events(
id_evento text PRIMARY KEY,
nombre_evento text NOT NULL,
posicion integer NOT NULL
);

-- aqui se crea la tabla de ocurrencias, cada fecha de un evento con sus asientos como contadores
CREATE TABLE IF NOT EXISTS srb.ocurrencias(
id_evento text NOT NULL REFERENCES srb.events(id_evento),
id_ocurrencia integer NOT NULL CHECK (id_ocurrencia >=1),
fecha date NOT NULL,
capacidad_total integer NOT NULL CHECK (capacidad_total >=1),
asientos_disponibles integer NOT NULL,
asientos_vendidos integer NOT NULL DEFAULT 0,
-- la llave es el evento junto con el numero de fecha
PRIMARY KEY (id_evento, id_ocurrencia),
-- red de seguridad, nunca asientos negativos
CONSTRAINT asientos_no_negativos CHECK (asientos_disponibles >= 0 AND asientos_vendidos >= 0),
-- red de seguridad, libres mas vendidos siempre suman la capacidad
CONSTRAINT asientos_validos CHECK (asientos_disponibles + asientos_vendidos = capacidad_total)
);

-- aqui se crea el contador que numera las reservas R0001 R0002 y demas
CREATE SEQUENCE IF NOT EXISTS srb.reservas_seq;

-- aqui se crea la tabla de reservas, quien, que evento y fecha, y cuantos asientos
CREATE TABLE IF NOT EXISTS srb.reservas(
id_reserva text PRIMARY KEY,
id_cliente text NOT NULL,
id_evento text NOT NULL,
id_ocurrencia integer NOT NULL,
cantidad_asientos integer NOT NULL CHECK (cantidad_asientos > 0),
-- el estado ya contempla cancelada para la segunda entrega
estado text NOT NULL DEFAULT 'ACTIVA' CHECK (estado IN ('ACTIVA', 'CANCELADA')),
creada_en timestamptz NOT NULL DEFAULT now(),
-- la reserva debe apuntar a una fecha que exista
FOREIGN KEY (id_evento, id_ocurrencia) REFERENCES srb.ocurrencias (id_evento, id_ocurrencia)
);

-- aqui se crea el indice para encontrar rapido las reservas de un cliente
CREATE INDEX IF NOT EXISTS reservas_por_cliente ON srb.reservas (id_cliente);

-- aqui se crea el libro de solicitudes ya ejecutadas con su respuesta, da la idempotencia
CREATE TABLE IF NOT EXISTS srb.solicitudes_procesadas(
id_solicitud text PRIMARY KEY,
operacion text NOT NULL,
respuesta jsonb,
procesada_en timestamptz NOT NULL DEFAULT now()
);

-- aqui se crea la vista que junta eventos con fechas y calcula la ocupacion, no guarda datos
CREATE OR REPLACE VIEW srb.vista_ocupacion AS
SELECT
e.posicion, e.id_evento, e.nombre_evento, o.id_ocurrencia, o.fecha, o.capacidad_total, o.asientos_disponibles, o.asientos_vendidos, round(100.0 * o.asientos_vendidos / o.capacidad_total, 1) AS porcentaje_ocupacion
FROM srb.events e
JOIN srb.ocurrencias o ON e.id_evento = o.id_evento
ORDER BY e.posicion, o.id_ocurrencia;

-- aqui se crea la funcion que arma el json de un rechazo con la forma de Respuesta en go
CREATE OR REPLACE FUNCTION srb.rechazo(p_id_solicitud text, p_mensaje text)
RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
SELECT jsonb_build_object('id_solicitud', p_id_solicitud, 'exito', false, 'mensaje', p_mensaje);
$$;

-- aqui se crea la funcion que pasa una reserva a json con la forma de Reserva en go
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

-- aqui se crea crear_reserva, la funcion que mantiene la consistencia y corre como una sola transaccion.
-- da igual cuantos workers o instancias de persistencia la llamen a la vez, nunca hay sobreventa
CREATE OR REPLACE FUNCTION srb.crear_reserva(
p_id_solicitud text,
p_id_cliente text,
p_id_evento text,
p_id_ocurrencia integer,
p_cantidad integer)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
-- respuesta que se va a devolver
v_respuesta jsonb;
-- asientos libres despues de restar
v_disponibles integer;
-- numero de la nueva reserva
v_numero bigint;
-- la reserva creada
v_reserva srb.reservas;
BEGIN
-- paso 0, la cantidad debe ser al menos 1
IF p_cantidad IS NULL OR p_cantidad < 1 THEN
    RETURN srb.rechazo(p_id_solicitud, 'hay que pedir por lo menos 1 asiento');
END IF;

-- paso 1, idempotencia, intenta registrar la solicitud y si ya existia no inserta nada
INSERT INTO srb.solicitudes_procesadas (id_solicitud, operacion)
VALUES (p_id_solicitud, 'crear_reserva')
ON CONFLICT (id_solicitud) DO NOTHING;
-- si ya existia devuelve la respuesta guardada sin tocar los asientos
IF NOT FOUND THEN
SELECT respuesta INTO v_respuesta FROM srb.solicitudes_procesadas WHERE id_solicitud = p_id_solicitud;
RETURN v_respuesta;
END IF;

-- paso 2, resta los asientos solo si alcanzan, revisar y restar pasa en la misma instruccion
-- con la fila bloqueada, asi dos clientes nunca se llevan los mismos asientos
UPDATE srb.ocurrencias
    SET asientos_disponibles = asientos_disponibles - p_cantidad,
        asientos_vendidos = asientos_vendidos + p_cantidad
    WHERE id_evento = p_id_evento
      AND id_ocurrencia = p_id_ocurrencia
      AND asientos_disponibles >= p_cantidad
    RETURNING asientos_disponibles INTO v_disponibles;

-- si no se modifico ninguna fila es porque no existe o porque no alcanzan
IF NOT FOUND THEN
        -- consulta cuantos quedan para saber cual de las dos fue
        SELECT asientos_disponibles INTO v_disponibles
          FROM srb.ocurrencias
         WHERE id_evento = p_id_evento AND id_ocurrencia = p_id_ocurrencia;
        IF v_disponibles IS NULL THEN
            -- el evento o la fecha no existen
            v_respuesta := srb.rechazo(p_id_solicitud, format('no existe %s con fecha %s', p_id_evento, p_id_ocurrencia));
        ELSE
            -- existen pero no alcanzan los asientos
            v_respuesta := srb.rechazo(p_id_solicitud, format('no alcanzan los asientos de %s en la fecha %s, pidio %s y %s', p_id_evento, p_id_ocurrencia, CASE WHEN p_cantidad = 1 THEN '1 asiento' ELSE p_cantidad || ' asientos' END, CASE v_disponibles WHEN 0 THEN 'no queda ninguno' WHEN 1 THEN 'solo queda 1' ELSE 'solo quedan ' || v_disponibles END));
        END IF;
    ELSE
        -- paso 3, toma el siguiente numero de reserva
        v_numero := nextval('srb.reservas_seq');
        -- aqui se crea la reserva con su numero R0001 y demas
        INSERT INTO srb.reservas (id_reserva, id_cliente, id_evento, id_ocurrencia, cantidad_asientos)
        VALUES ('R' || lpad(v_numero::text, greatest(4, length(v_numero::text)), '0'), p_id_cliente, p_id_evento, p_id_ocurrencia, p_cantidad)
        RETURNING * INTO v_reserva;
        -- aqui se crea el json de exito con la reserva
        v_respuesta := jsonb_build_object(
            'id_solicitud', p_id_solicitud,
            'exito', true,
            'mensaje', format('reserva %s lista, %s de %s en la fecha %s, %s', v_reserva.id_reserva, CASE WHEN p_cantidad = 1 THEN '1 asiento' ELSE p_cantidad || ' asientos' END, p_id_evento, p_id_ocurrencia, CASE v_disponibles WHEN 0 THEN 'ya no quedan libres' WHEN 1 THEN 'queda 1 libre' ELSE 'quedan ' || v_disponibles || ' libres' END),
            'reserva', srb.reserva_json(v_reserva));
    END IF;

    -- paso 4, guarda la respuesta en el libro por si la solicitud llega repetida
    UPDATE srb.solicitudes_procesadas SET respuesta = v_respuesta WHERE id_solicitud = p_id_solicitud;
    -- devuelve la respuesta
    RETURN v_respuesta;
END;
$$;
