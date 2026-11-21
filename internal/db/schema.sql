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
v_respuesta jsonb;
v_disponibles integer;
v_numero bigint;
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
    RETURN v_respuesta;
END;
$$;

-- aqui se crea modificar_reserva, cambia los asientos o la fecha de una reserva en una sola transaccion distribuida.
-- si la modificacion no se puede hacer se rechaza y la reserva queda exactamente como estaba
CREATE OR REPLACE FUNCTION srb.modificar_reserva(
p_id_solicitud text,
p_id_cliente text,
p_id_reserva text,
p_nueva_cantidad integer,
p_nueva_ocurrencia integer)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
-- respuesta que se va a devolver
v_respuesta jsonb;
-- la reserva antes y despues del cambio
v_reserva srb.reservas;
-- diferencia de asientos cuando se cambia la cantidad
v_diferencia integer;
-- cuantas fechas tiene el evento
v_fechas integer;
-- asientos libres de la fecha despues del cambio
v_disponibles integer;
BEGIN
-- paso 0, se cambia una sola cosa, los asientos o la fecha
IF (coalesce(p_nueva_cantidad, 0) > 0) = (coalesce(p_nueva_ocurrencia, 0) > 0) THEN
    RETURN srb.rechazo(p_id_solicitud, 'hay que cambiar los asientos o la fecha, una sola de las dos');
END IF;
-- paso 1, idempotencia, igual que en crear_reserva
INSERT INTO srb.solicitudes_procesadas (id_solicitud, operacion)
VALUES (p_id_solicitud, 'modificar_reserva')
ON CONFLICT (id_solicitud) DO NOTHING;
-- si ya existia devuelve la respuesta guardada sin tocar nada
IF NOT FOUND THEN
SELECT respuesta INTO v_respuesta FROM srb.solicitudes_procesadas WHERE id_solicitud = p_id_solicitud;
RETURN v_respuesta;
END IF;

-- paso 2, bloquea la reserva para que nadie la cambie o la cancele al mismo tiempo
SELECT * INTO v_reserva FROM srb.reservas WHERE id_reserva = p_id_reserva FOR UPDATE;
IF NOT FOUND THEN
    v_respuesta := srb.rechazo(p_id_solicitud, format('no existe la reserva %s', p_id_reserva));
ELSIF v_reserva.id_cliente <> p_id_cliente THEN
    -- un cliente solo puede tocar sus propias reservas
    v_respuesta := srb.rechazo(p_id_solicitud, format('la reserva %s no es de %s', p_id_reserva, p_id_cliente));
ELSIF v_reserva.estado <> 'ACTIVA' THEN
    v_respuesta := srb.rechazo(p_id_solicitud, format('la reserva %s esta cancelada, no se puede modificar', p_id_reserva));
END IF;

-- paso 3, se bloquean las fechas del evento siempre en el mismo orden, asi dos modificaciones
-- que cruzan fechas al mismo tiempo no se quedan esperandose una a la otra y no se produce deadlock
IF v_respuesta IS NULL THEN
    PERFORM 1 FROM srb.ocurrencias WHERE id_evento = v_reserva.id_evento ORDER BY id_ocurrencia FOR UPDATE;
END IF;

-- paso 4a, cambio de asientos en la misma fecha
IF v_respuesta IS NULL AND coalesce(p_nueva_cantidad, 0) > 0 THEN
    v_diferencia := p_nueva_cantidad - v_reserva.cantidad_asientos;
    IF v_diferencia = 0 THEN
        v_respuesta := srb.rechazo(p_id_solicitud, format('la reserva %s ya tiene %s', p_id_reserva, CASE WHEN p_nueva_cantidad = 1 THEN '1 asiento' ELSE p_nueva_cantidad || ' asientos' END));
    ELSE
        -- si pide mas resta los que faltan solo si alcanzan, si pide menos devuelve los que sobran
        UPDATE srb.ocurrencias
           SET asientos_disponibles = asientos_disponibles - v_diferencia,
               asientos_vendidos = asientos_vendidos + v_diferencia
         WHERE id_evento = v_reserva.id_evento
           AND id_ocurrencia = v_reserva.id_ocurrencia
           AND asientos_disponibles >= v_diferencia
        RETURNING asientos_disponibles INTO v_disponibles;
        IF NOT FOUND THEN
            -- no alcanzan, no se cambio nada
            SELECT asientos_disponibles INTO v_disponibles FROM srb.ocurrencias
             WHERE id_evento = v_reserva.id_evento AND id_ocurrencia = v_reserva.id_ocurrencia;
            v_respuesta := srb.rechazo(p_id_solicitud, format('no alcanzan los asientos para dejar la reserva %s en %s, faltan %s y %s', p_id_reserva, p_nueva_cantidad, v_diferencia, CASE v_disponibles WHEN 0 THEN 'no queda ninguno libre' WHEN 1 THEN 'solo queda 1 libre' ELSE 'solo quedan ' || v_disponibles || ' libres' END));
        ELSE
            -- se guarda la nueva cantidad en la reserva
            UPDATE srb.reservas SET cantidad_asientos = p_nueva_cantidad WHERE id_reserva = p_id_reserva RETURNING * INTO v_reserva;
            v_respuesta := jsonb_build_object(
                'id_solicitud', p_id_solicitud,
                'exito', true,
                'mensaje', format('reserva %s modificada, ahora tiene %s de %s en la fecha %s', p_id_reserva, CASE WHEN p_nueva_cantidad = 1 THEN '1 asiento' ELSE p_nueva_cantidad || ' asientos' END, v_reserva.id_evento, v_reserva.id_ocurrencia),
                'reserva', srb.reserva_json(v_reserva));
        END IF;
    END IF;
END IF;

-- paso 4b, cambio de fecha conservando el numero de asientos
IF v_respuesta IS NULL AND coalesce(p_nueva_ocurrencia, 0) > 0 THEN
    SELECT count(*) INTO v_fechas FROM srb.ocurrencias WHERE id_evento = v_reserva.id_evento;
    IF v_fechas < 2 THEN
        -- regla del enunciado, si el evento tiene una sola fecha solo se cambian los asientos
        v_respuesta := srb.rechazo(p_id_solicitud, format('%s tiene una sola fecha en el mes, solo se puede cambiar el numero de asientos', v_reserva.id_evento));
    ELSIF p_nueva_ocurrencia = v_reserva.id_ocurrencia THEN
        v_respuesta := srb.rechazo(p_id_solicitud, format('la reserva %s ya es para la fecha %s', p_id_reserva, p_nueva_ocurrencia));
    ELSE
        -- ocupa los asientos en la nueva fecha solo si alcanzan
        UPDATE srb.ocurrencias
           SET asientos_disponibles = asientos_disponibles - v_reserva.cantidad_asientos,
               asientos_vendidos = asientos_vendidos + v_reserva.cantidad_asientos
         WHERE id_evento = v_reserva.id_evento
           AND id_ocurrencia = p_nueva_ocurrencia
           AND asientos_disponibles >= v_reserva.cantidad_asientos
        RETURNING asientos_disponibles INTO v_disponibles;
        IF NOT FOUND THEN
            -- la fecha no existe o no alcanzan, no se cambio nada
            SELECT asientos_disponibles INTO v_disponibles FROM srb.ocurrencias
             WHERE id_evento = v_reserva.id_evento AND id_ocurrencia = p_nueva_ocurrencia;
            IF v_disponibles IS NULL THEN
                v_respuesta := srb.rechazo(p_id_solicitud, format('no existe %s con fecha %s', v_reserva.id_evento, p_nueva_ocurrencia));
            ELSE
                v_respuesta := srb.rechazo(p_id_solicitud, format('no alcanzan los asientos de %s en la fecha %s, la reserva tiene %s y %s', v_reserva.id_evento, p_nueva_ocurrencia, v_reserva.cantidad_asientos, CASE v_disponibles WHEN 0 THEN 'no queda ninguno' WHEN 1 THEN 'solo queda 1' ELSE 'solo quedan ' || v_disponibles END));
            END IF;
        ELSE
            -- libera los asientos de la fecha vieja
            UPDATE srb.ocurrencias
               SET asientos_disponibles = asientos_disponibles + v_reserva.cantidad_asientos,
                   asientos_vendidos = asientos_vendidos - v_reserva.cantidad_asientos
             WHERE id_evento = v_reserva.id_evento AND id_ocurrencia = v_reserva.id_ocurrencia;
            -- mueve la reserva a la nueva fecha
            UPDATE srb.reservas SET id_ocurrencia = p_nueva_ocurrencia WHERE id_reserva = p_id_reserva RETURNING * INTO v_reserva;
            v_respuesta := jsonb_build_object(
                'id_solicitud', p_id_solicitud,
                'exito', true,
                'mensaje', format('reserva %s modificada, ahora es para la fecha %s de %s con %s', p_id_reserva, p_nueva_ocurrencia, v_reserva.id_evento, CASE WHEN v_reserva.cantidad_asientos = 1 THEN '1 asiento' ELSE v_reserva.cantidad_asientos || ' asientos' END),
                'reserva', srb.reserva_json(v_reserva));
        END IF;
    END IF;
END IF;

-- paso 5, guarda la respuesta en el libro por si la solicitud llega repetida
UPDATE srb.solicitudes_procesadas SET respuesta = v_respuesta WHERE id_solicitud = p_id_solicitud;
RETURN v_respuesta;
END;
$$;

-- aqui se crea cancelar_reserva, libera los asientos y marca la reserva como cancelada en una sola transaccion
CREATE OR REPLACE FUNCTION srb.cancelar_reserva(
p_id_solicitud text,
p_id_cliente text,
p_id_reserva text)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
-- respuesta que se va a devolver
v_respuesta jsonb;
-- la reserva a cancelar
v_reserva srb.reservas;
-- asientos libres despues de liberar
v_disponibles integer;
BEGIN
-- paso 1, idempotencia, si la cancelacion llega repetida no se liberan los asientos dos veces
INSERT INTO srb.solicitudes_procesadas (id_solicitud, operacion)
VALUES (p_id_solicitud, 'cancelar_reserva')
ON CONFLICT (id_solicitud) DO NOTHING;
IF NOT FOUND THEN
SELECT respuesta INTO v_respuesta FROM srb.solicitudes_procesadas WHERE id_solicitud = p_id_solicitud;
RETURN v_respuesta;
END IF;

-- paso 2, bloquea la reserva para que nadie la modifique mientras se cancela
SELECT * INTO v_reserva FROM srb.reservas WHERE id_reserva = p_id_reserva FOR UPDATE;
IF NOT FOUND THEN
    v_respuesta := srb.rechazo(p_id_solicitud, format('no existe la reserva %s', p_id_reserva));
ELSIF v_reserva.id_cliente <> p_id_cliente THEN
    -- un cliente solo puede cancelar sus propias reservas
    v_respuesta := srb.rechazo(p_id_solicitud, format('la reserva %s no es de %s', p_id_reserva, p_id_cliente));
ELSIF v_reserva.estado <> 'ACTIVA' THEN
    v_respuesta := srb.rechazo(p_id_solicitud, format('la reserva %s ya estaba cancelada', p_id_reserva));
ELSE
    -- paso 3, devuelve los asientos a la fecha
    UPDATE srb.ocurrencias
       SET asientos_disponibles = asientos_disponibles + v_reserva.cantidad_asientos,
           asientos_vendidos = asientos_vendidos - v_reserva.cantidad_asientos
     WHERE id_evento = v_reserva.id_evento AND id_ocurrencia = v_reserva.id_ocurrencia
    RETURNING asientos_disponibles INTO v_disponibles;
    -- paso 4, marca la reserva como cancelada, no se borra para que quede el historial
    UPDATE srb.reservas SET estado = 'CANCELADA' WHERE id_reserva = p_id_reserva RETURNING * INTO v_reserva;
    v_respuesta := jsonb_build_object(
        'id_solicitud', p_id_solicitud,
        'exito', true,
        'mensaje', format('reserva %s cancelada, se liberaron %s de %s en la fecha %s, ahora quedan %s libres', p_id_reserva, CASE WHEN v_reserva.cantidad_asientos = 1 THEN '1 asiento' ELSE v_reserva.cantidad_asientos || ' asientos' END, v_reserva.id_evento, v_reserva.id_ocurrencia, v_disponibles),
        'reserva', srb.reserva_json(v_reserva));
END IF;

-- paso 5, guarda la respuesta en el libro por si la cancelacion llega repetida
UPDATE srb.solicitudes_procesadas SET respuesta = v_respuesta WHERE id_solicitud = p_id_solicitud;
RETURN v_respuesta;
END;
$$;
