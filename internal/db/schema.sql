-- esquema de la base de datos (postgresql en supabase)
--
-- este archivo se mete dentro del ejecutable (//go:embed en postgres.go) y se
-- corre cada vez que arranca persistencia o se usa -ver. por eso todo usa
-- "if not exists" / "or replace": si ya existe no pasa nada y si falta se crea.
-- los comentarios en sql empiezan con dos guiones.
--
-- todas las tablas viven en un esquema propio que se llama srb (una carpeta
-- logica dentro de la base). por eso en supabase hay que cambiar el selector de
-- "public" a "srb" para verlas.
--
-- relacion entre tablas:
--   events (1) ----< ocurrencias (1) ----< reservas
--   solicitudes_procesadas: tabla aparte, solo para no repetir solicitudes.
CREATE SCHEMA IF NOT EXISTS srb;

-- eventos: uno por evento (E1, E2, ...). "posicion" guarda el orden en que se
-- cargaron para que las consultas salgan siempre en el mismo orden.
CREATE TABLE IF NOT EXISTS srb.events(
id_evento text PRIMARY KEY,
nombre_evento text NOT NULL,
posicion integer NOT NULL
);

-- ocurrencias: cada fecha de un evento (1 o 2 por evento) con sus asientos.
-- aqui esta el inventario: los asientos son contadores (disponibles y vendidos),
-- no sillas individuales numeradas.
-- la llave primaria es la combinacion (evento, numero de ocurrencia).
CREATE TABLE IF NOT EXISTS srb.ocurrencias(
id_evento text NOT NULL REFERENCES srb.events(id_evento),
id_ocurrencia integer NOT NULL CHECK (id_ocurrencia >=1),
fecha date NOT NULL,
capacidad_total integer NOT NULL CHECK (capacidad_total >=1),
asientos_disponibles integer NOT NULL,
asientos_vendidos integer NOT NULL DEFAULT 0,
PRIMARY KEY (id_evento, id_ocurrencia),
-- estos dos check son una red de seguridad: postgresql rechaza cualquier cambio
-- que deje asientos negativos o que haga que disponibles + vendidos deje de
-- sumar la capacidad. o sea que ni un error de programacion deja vender de mas.
CONSTRAINT asientos_no_negativos CHECK (asientos_disponibles >= 0 AND asientos_vendidos >= 0),
CONSTRAINT asientos_validos CHECK (asientos_disponibles + asientos_vendidos = capacidad_total)
);

-- contador para numerar las reservas: R0001, R0002, ... (se reinicia en reiniciar).
CREATE SEQUENCE IF NOT EXISTS srb.reservas_seq;

-- reservas: cada reserva hecha. guarda quien (id_cliente), que (evento y fecha)
-- y cuantos asientos. el estado ya contempla 'CANCELADA' para la segunda entrega.
CREATE TABLE IF NOT EXISTS srb.reservas(
id_reserva text PRIMARY KEY,
id_cliente text NOT NULL,
id_evento text NOT NULL,
id_ocurrencia integer NOT NULL,
cantidad_asientos integer NOT NULL CHECK (cantidad_asientos > 0),
estado text NOT NULL DEFAULT 'ACTIVA' CHECK (estado IN ('ACTIVA', 'CANCELADA')),
creada_en timestamptz NOT NULL DEFAULT now(),
FOREIGN KEY (id_evento, id_ocurrencia) REFERENCES srb.ocurrencias (id_evento, id_ocurrencia)
);

-- indice para encontrar rapido las reservas de un cliente.
CREATE INDEX IF NOT EXISTS reservas_por_cliente ON srb.reservas (id_cliente);

-- solicitudes procesadas: un libro con cada solicitud ya ejecutada y la
-- respuesta que se dio. es lo que permite la idempotencia: si llega otra vez el
-- mismo id_solicitud no se ejecuta de nuevo, se devuelve la respuesta guardada.
CREATE TABLE IF NOT EXISTS srb.solicitudes_procesadas(
id_solicitud text PRIMARY KEY,
operacion text NOT NULL,
respuesta jsonb,
procesada_en timestamptz NOT NULL DEFAULT now()
);

-- vista: una "tabla virtual" que junta events con ocurrencias y calcula el
-- porcentaje de ocupacion. no guarda datos propios, se recalcula cada vez que se
-- consulta. es lo que lee consultareventos y lo que se ve en supabase.
CREATE OR REPLACE VIEW srb.vista_ocupacion AS
SELECT
e.posicion, e.id_evento, e.nombre_evento, o.id_ocurrencia, o.fecha, o.capacidad_total, o.asientos_disponibles, o.asientos_vendidos, round(100.0 * o.asientos_vendidos / o.capacidad_total, 1) AS porcentaje_ocupacion
FROM srb.events e
JOIN srb.ocurrencias o ON e.id_evento = o.id_evento
ORDER BY e.posicion, o.id_ocurrencia;

-- funcion auxiliar: arma el json de una respuesta de rechazo.
-- el resultado tiene la misma forma que el struct respuesta de go
-- (id_solicitud, exito, mensaje), por eso go lo puede leer directo.
CREATE OR REPLACE FUNCTION srb.rechazo(p_id_solicitud text, p_mensaje text)
RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
SELECT jsonb_build_object('id_solicitud', p_id_solicitud, 'exito', false, 'mensaje', p_mensaje);
$$;

-- funcion auxiliar: pasa una fila de reservas a json (el struct reserva de go).
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

-- funcion principal: crear_reserva. es lo que mantiene la consistencia del sistema.
-- se llama desde go con SELECT srb.crear_reserva(...). como es una sola llamada,
-- postgresql la corre como una unica transaccion: o todo o nada.
-- devuelve un json con el resultado (exito o rechazo).
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
-- paso 0: validacion basica de la cantidad.
IF p_cantidad IS NULL OR p_cantidad < 1 THEN
    RETURN srb.rechazo(p_id_solicitud, 'hay que pedir por lo menos 1 asiento');
END IF;

-- paso 1: idempotencia. se intenta registrar la solicitud. si ese id_solicitud
-- ya existia, el do nothing no inserta nada y found queda en falso: en ese caso
-- se devuelve la respuesta que se guardo la vez anterior y se termina, sin tocar
-- los asientos. (si llegan dos copias a la vez, la segunda espera a que la
-- primera termine.)
INSERT INTO srb.solicitudes_procesadas (id_solicitud, operacion)
VALUES (p_id_solicitud, 'crear_reserva')
ON CONFLICT (id_solicitud) DO NOTHING;
IF NOT FOUND THEN
SELECT respuesta INTO v_respuesta FROM srb.solicitudes_procesadas WHERE id_solicitud = p_id_solicitud;
RETURN v_respuesta;
END IF;

-- paso 2: descontar los asientos de forma atomica. este update solo toca la fila
-- si todavia quedan asientos suficientes (asientos_disponibles >= p_cantidad).
-- revisar y restar pasa en la misma instruccion, con la fila bloqueada, asi que
-- dos clientes al tiempo nunca se pueden llevar los mismos asientos. el
-- returning guarda en v_disponibles cuantos quedan despues de restar.
UPDATE srb.ocurrencias
    SET asientos_disponibles = asientos_disponibles - p_cantidad,
        asientos_vendidos = asientos_vendidos + p_cantidad
    WHERE id_evento = p_id_evento
      AND id_ocurrencia = p_id_ocurrencia
      AND asientos_disponibles >= p_cantidad
    RETURNING asientos_disponibles INTO v_disponibles;

-- si el update no modifico ninguna fila (found en falso) fue por una de dos razones:
IF NOT FOUND THEN
        -- se consulta cuantos asientos quedan para saber cual de las dos fue.
        SELECT asientos_disponibles INTO v_disponibles
          FROM srb.ocurrencias
         WHERE id_evento = p_id_evento AND id_ocurrencia = p_id_ocurrencia;
        IF v_disponibles IS NULL THEN
            -- (a) el evento o la fecha no existen.
            v_respuesta := srb.rechazo(p_id_solicitud, format('no existe %s con fecha %s', p_id_evento, p_id_ocurrencia));
        ELSE
            -- (b) existen pero no alcanzan los asientos.
            v_respuesta := srb.rechazo(p_id_solicitud, format('no alcanzan los asientos de %s en la fecha %s, pidio %s y %s', p_id_evento, p_id_ocurrencia, CASE WHEN p_cantidad = 1 THEN '1 asiento' ELSE p_cantidad || ' asientos' END, CASE v_disponibles WHEN 0 THEN 'no queda ninguno' WHEN 1 THEN 'solo queda 1' ELSE 'solo quedan ' || v_disponibles END));
        END IF;
    ELSE
        -- paso 3: el update si funciono, se registra la reserva con su numero (R0001...).
        v_numero := nextval('srb.reservas_seq');
        INSERT INTO srb.reservas (id_reserva, id_cliente, id_evento, id_ocurrencia, cantidad_asientos)
        VALUES ('R' || lpad(v_numero::text, greatest(4, length(v_numero::text)), '0'), p_id_cliente, p_id_evento, p_id_ocurrencia, p_cantidad)
        RETURNING * INTO v_reserva;
        -- se arma el json de exito, que trae la reserva creada.
        v_respuesta := jsonb_build_object(
            'id_solicitud', p_id_solicitud,
            'exito', true,
            'mensaje', format('reserva %s lista, %s de %s en la fecha %s, %s', v_reserva.id_reserva, CASE WHEN p_cantidad = 1 THEN '1 asiento' ELSE p_cantidad || ' asientos' END, p_id_evento, p_id_ocurrencia, CASE v_disponibles WHEN 0 THEN 'ya no quedan libres' WHEN 1 THEN 'queda 1 libre' ELSE 'quedan ' || v_disponibles || ' libres' END),
            'reserva', srb.reserva_json(v_reserva));
    END IF;

    -- paso 4: se guarda la respuesta en el libro de solicitudes (para devolverla
    -- igual si la misma solicitud llega repetida) y se entrega.
    UPDATE srb.solicitudes_procesadas SET respuesta = v_respuesta WHERE id_solicitud = p_id_solicitud;
    RETURN v_respuesta;
END;
$$;
