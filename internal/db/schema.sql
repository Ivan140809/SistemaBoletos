-- ESQUEMA DE LA BASE DE DATOS (PostgreSQL en Supabase)
--
-- Este archivo se mete dentro del ejecutable (//go:embed en postgres.go) y se
-- ejecuta cada vez que arranca persistencia o se usa -ver. Por eso todo usa
-- "IF NOT EXISTS" / "OR REPLACE": si ya existe, no pasa nada; si falta, se crea.
-- Los comentarios en SQL empiezan con dos guiones.
--
-- Todas las tablas viven en un "esquema" propio llamado srb (una carpeta lógica
-- dentro de la base). Por eso en Supabase hay que cambiar el selector de
-- "public" a "srb" para verlas.
--
-- Relación entre tablas:
--   events (1) ----< ocurrencias (1) ----< reservas
--   solicitudes_procesadas: tabla aparte, solo para no repetir solicitudes.
CREATE SCHEMA IF NOT EXISTS srb;

-- EVENTOS: uno por evento (E1, E2, ...). "posicion" conserva el orden en que
-- se cargaron para que las consultas salgan siempre ordenadas igual.
CREATE TABLE IF NOT EXISTS srb.events(
id_evento text PRIMARY KEY,
nombre_evento text NOT NULL,
posicion integer NOT NULL
);

-- OCURRENCIAS: cada fecha de un evento (1 o 2 por evento) con sus asientos.
-- Aquí está el "inventario": los asientos son CONTADORES (disponibles y
-- vendidos), no sillas individuales numeradas.
-- La llave primaria es la combinación (evento, número de ocurrencia).
CREATE TABLE IF NOT EXISTS srb.ocurrencias(
id_evento text NOT NULL REFERENCES srb.events(id_evento),
id_ocurrencia integer NOT NULL CHECK (id_ocurrencia >=1),
fecha date NOT NULL,
capacidad_total integer NOT NULL CHECK (capacidad_total >=1),
asientos_disponibles integer NOT NULL,
asientos_vendidos integer NOT NULL DEFAULT 0,
PRIMARY KEY (id_evento, id_ocurrencia),
-- Estas dos restricciones (CHECK) son una red de seguridad: PostgreSQL
-- rechaza cualquier cambio que deje asientos negativos o que haga que
-- disponibles + vendidos deje de sumar la capacidad. Es decir, ni un error
-- de programación permite vender de más.
CONSTRAINT asientos_no_negativos CHECK (asientos_disponibles >= 0 AND asientos_vendidos >= 0),
CONSTRAINT asientos_validos CHECK (asientos_disponibles + asientos_vendidos = capacidad_total)
);

-- Contador para numerar las reservas: R0001, R0002, ... (se reinicia en Reiniciar).
CREATE SEQUENCE IF NOT EXISTS srb.reservas_seq;

-- RESERVAS: cada reserva hecha. Guarda quién (id_cliente), qué (evento y fecha)
-- y cuántos asientos. El estado ya contempla 'CANCELADA' para la segunda entrega.
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

-- Índice para buscar rápido las reservas de un cliente.
CREATE INDEX IF NOT EXISTS reservas_por_cliente ON srb.reservas (id_cliente);

-- SOLICITUDES PROCESADAS: libro de registro de cada solicitud ya ejecutada,
-- con la respuesta que se dio. Es lo que hace posible la IDEMPOTENCIA: si
-- llega otra vez el mismo id_solicitud, no se vuelve a ejecutar; se devuelve
-- la respuesta guardada.
CREATE TABLE IF NOT EXISTS srb.solicitudes_procesadas(
id_solicitud text PRIMARY KEY,
operacion text NOT NULL,
respuesta jsonb,
procesada_en timestamptz NOT NULL DEFAULT now()
);

-- VISTA: una "tabla virtual" que une events con ocurrencias y calcula el
-- porcentaje de ocupación. No guarda datos propios; cada vez que se consulta
-- recalcula. Es lo que lee ConsultarEventos y lo que se ve en Supabase.
CREATE OR REPLACE VIEW srb.vista_ocupacion AS
SELECT
e.posicion, e.id_evento, e.nombre_evento, o.id_ocurrencia, o.fecha, o.capacidad_total, o.asientos_disponibles, o.asientos_vendidos, round(100.0 * o.asientos_vendidos / o.capacidad_total, 1) AS porcentaje_ocupacion
FROM srb.events e
JOIN srb.ocurrencias o ON e.id_evento = o.id_evento
ORDER BY e.posicion, o.id_ocurrencia;

-- FUNCIÓN auxiliar: arma el JSON de una respuesta de rechazo.
-- Su resultado tiene la misma forma que el struct Respuesta de Go
-- (id_solicitud, exito, mensaje), por eso Go puede leerlo directamente.
CREATE OR REPLACE FUNCTION srb.rechazo(p_id_solicitud text, p_mensaje text)
RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
SELECT jsonb_build_object('id_solicitud', p_id_solicitud, 'exito', false, 'mensaje', p_mensaje);
$$;

-- FUNCIÓN auxiliar: convierte una fila de reservas en JSON (el struct Reserva de Go).
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

-- FUNCIÓN PRINCIPAL: crear_reserva. Es el corazón de la consistencia del sistema.
-- Se llama desde Go con SELECT srb.crear_reserva(...). Como es UNA sola
-- llamada, PostgreSQL la ejecuta como una única transacción: o todo, o nada.
-- Devuelve un JSON con el resultado (éxito o rechazo).
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
-- Paso 0: validación básica de la cantidad.
IF p_cantidad IS NULL OR p_cantidad < 1 THEN
    RETURN srb.rechazo(p_id_solicitud, 'hay que pedir por lo menos 1 asiento');
END IF;

-- Paso 1: IDEMPOTENCIA. Se intenta registrar la solicitud. Si ese id_solicitud
-- ya existía, DO NOTHING no inserta nada y FOUND queda en falso: en ese caso se
-- devuelve la respuesta guardada la vez anterior y se termina, sin tocar asientos.
-- (Si dos copias llegan a la vez, la segunda espera a que termine la primera.)
INSERT INTO srb.solicitudes_procesadas (id_solicitud, operacion)
VALUES (p_id_solicitud, 'crear_reserva')
ON CONFLICT (id_solicitud) DO NOTHING;
IF NOT FOUND THEN
SELECT respuesta INTO v_respuesta FROM srb.solicitudes_procesadas WHERE id_solicitud = p_id_solicitud;
RETURN v_respuesta;
END IF;

-- Paso 2: DESCONTAR ASIENTOS de forma atómica. Este UPDATE solo afecta la fila
-- si todavía quedan suficientes asientos (asientos_disponibles >= p_cantidad).
-- La verificación y la resta ocurren en la MISMA instrucción, con la fila
-- bloqueada, así que dos clientes simultáneos nunca pueden llevarse los mismos
-- asientos. RETURNING guarda en v_disponibles cuántos quedan después de la resta.
UPDATE srb.ocurrencias
    SET asientos_disponibles = asientos_disponibles - p_cantidad,
        asientos_vendidos = asientos_vendidos + p_cantidad
    WHERE id_evento = p_id_evento
      AND id_ocurrencia = p_id_ocurrencia
      AND asientos_disponibles >= p_cantidad
    RETURNING asientos_disponibles INTO v_disponibles;

-- Si el UPDATE no modificó ninguna fila (FOUND en falso) fue por una de dos razones:
IF NOT FOUND THEN
        -- Se consulta cuántos asientos quedan para saber cuál de las dos fue.
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
        -- Paso 3: el UPDATE sí funcionó, se registra la reserva con su número (R0001...).
        v_numero := nextval('srb.reservas_seq');
        INSERT INTO srb.reservas (id_reserva, id_cliente, id_evento, id_ocurrencia, cantidad_asientos)
        VALUES ('R' || lpad(v_numero::text, greatest(4, length(v_numero::text)), '0'), p_id_cliente, p_id_evento, p_id_ocurrencia, p_cantidad)
        RETURNING * INTO v_reserva;
        -- Se arma el JSON de éxito, que incluye la reserva creada.
        v_respuesta := jsonb_build_object(
            'id_solicitud', p_id_solicitud,
            'exito', true,
            'mensaje', format('reserva %s lista, %s de %s en la fecha %s, %s', v_reserva.id_reserva, CASE WHEN p_cantidad = 1 THEN '1 asiento' ELSE p_cantidad || ' asientos' END, p_id_evento, p_id_ocurrencia, CASE v_disponibles WHEN 0 THEN 'ya no quedan libres' WHEN 1 THEN 'queda 1 libre' ELSE 'quedan ' || v_disponibles || ' libres' END),
            'reserva', srb.reserva_json(v_reserva));
    END IF;

    -- Paso 4: se guarda la respuesta en el libro de solicitudes (para devolverla
    -- tal cual si la misma solicitud llega repetida) y se entrega.
    UPDATE srb.solicitudes_procesadas SET respuesta = v_respuesta WHERE id_solicitud = p_id_solicitud;
    RETURN v_respuesta;
END;
$$;
