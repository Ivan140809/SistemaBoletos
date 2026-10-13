# SRB: Sistema de Reserva de Boletos

Sistema distribuido de tres capas para consultar eventos y reservar asientos, desarrollado como proyecto del curso Introducción a Sistemas Distribuidos. Los procesos están escritos en Go, se comunican mediante ZeroMQ, y el estado se persiste en PostgreSQL (Supabase).

Operaciones disponibles hasta el momento:

- **Consulta**: lista los eventos de un mes con sus asientos libres y vendidos.
- **Reserva**: reserva asientos de un evento en una fecha. Si no alcanzan, la reserva se rechaza y la base no cambia.

Se implementan las operaciones de consulta y reserva de forma sincrona.

## 1. Tecnologías

- **Go 1.25**: Todos los procesos del sistema.
- **ZeroMQ** (librería `pebbe/zmq4`): Comunicación entre procesos con los patrones REQ/ROUTER, DEALER/REP, REQ/REP y PUSH/PULL.
- **PostgreSQL en Supabase** (driver `pgx`): Eventos, ocurrencias y reservas. El esquema y la función de reserva están en `internal/db/schema.sql`.
- **Scripts de bash**: Arranque cada máquina.

---

## 2. Propuesta de arquitectura base

El sistema cuenta con tres capas. Cada una corre en su propia máquina:

| Máquina | Capa | Donde esta en el programa | Qué hace |
|---|---|---|---|
| **1** | Consumidores | `client` | Envía consultas y reservas |
| **2** | Negocio | `manager` (Gestor de Transacciones, GT), `rmc` | El GT recibe las solicitudes, las valida y se las pasa a RMC. RMC consulta la disponibilidad y confirma la reserva. |
| **3** | Almacenamiento | `persistence` | Es el único proceso que habla con la base de datos en supabase

Comunicación entre procesos:

| Origen | Destino | Patron | Puerto|
|---|---|---|---|
| `client` (M1) | `manager` (M2) | REQ / ROUTER | 5555 en M2 |
| `manager` (M2) | `rmc` (M2) | DEALER / REP | 5556 en M2 |
| `manager` (M2) | `cancellations` (M2) | PUSH / PULL | 5557 en M2 |
| `rmc` (M2) | `persistence` (M3) | REQ / REP | 5560 en M3 |
| `persistence` (M3) | Supabase | PostgreSQL | 5432 en Supabase |

Flujo de una reserva: el cliente envía la solicitud al GT. GT la valida y la reenvía a RMC. RMC consulta a Persistencia cuántos asientos quedan y, si alcanzan, le solicita registrar la reserva. La respuesta regresa por el mismo camino.

### Estructura del proyecto

| Ruta | Contenido |
|---|---|
| `cmd/client/` | Cliente (M1) |
| `cmd/manager/` | Gestor de Transacciones (M2) |
| `cmd/rmc/` | Servicio de reservas y consultas (M2) |
| `cmd/cancellations/` | Servicio de cancelaciones (M2) |
| `cmd/persistence/` | Servicio de persistencia (M3) |
| `internal/model/` | Estructuras compartidas: solicitud, respuesta, evento, ocurrencia, reserva |
| `internal/communication/` | Cliente REQ con tiempo de espera, lo usan el cliente y RMC |
| `internal/db/` | Acceso a PostgreSQL y esquema de la base (`schema.sql`) |
| `data/test_case.json` | Eventos con los que arranca la base |
| `scripts/` | Scripts para arrancar cada máquina |
| `.env.example` | Plantilla del archivo con la conexión a la base |

---

## 3. Requisitos

- **Go 1.25 o superior**
- **libzmq**, **pkg-config** y **gcc**: la librería de ZeroMQ para Go (pebbe/zmq4) se compila contra libzmq
- **git**, para descargar el código

### Ubuntu / Debian

```bash
sudo apt update
sudo apt install -y libzmq3-dev pkg-config build-essential git
```
## 4. Descarga y compilación

```bash
git clone https://github.com/Ivan140809/SistemaBoletos.git
cd SistemaBoletos
go mod download
go build -o bin/ ./cmd/...
```

Si recibió el proyecto en un `.zip`, descomprímalo y ejecute los tres últimos comandos dentro de la carpeta.

Al terminar debe existir la carpeta `bin/` con cinco programas:

```bash
ls bin/
```
---

## 5. Configuración de la base de datos

En M3, dentro de la carpeta del proyecto:

```bash
cp .env.example .env
```

Abra `.env` y deje la primera línea así, con la cadena de conexión de Supabase (en el proyecto: **Connect**, opción **Session pooler**):

```bash
DATABASE_URL='postgresql://postgres.abcdefghijk:SuPassword123@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require'
```
- **Agregue `?sslmode=require`** al final.
---

## 6. Conectar las máquinas

### 6.1 Averiguar las IP

En cada máquina:

```bash
hostname -I
```

Si ese comando no existe, use `ip -4 addr` y busque la línea `inet` de la tarjeta de red (no la de `lo`).

Anote la IP de **M2** y la de **M3**. En este documento aparecen como `<IP_M2>` y `<IP_M3>`. M1 no necesita que nadie se conecte a ella.

Si son máquinas virtuales (VirtualBox, VMware, UTM), configure la red en modo **Adaptador puente** o en una **Red NAT** compartida, para que las tres se vean entre sí. Compruébelo desde M1 y desde M2:

```bash
ping -c 3 <IP_M2>
ping -c 3 <IP_M3>
```

### 6.2 Abrir los puertos

| Máquina | Puertos | Para qué |
|---|---|---|
| M2 | 5555, 5556, 5557 | Clientes, RMC y Cancelaciones se conectan al GT |
| M3 | 5560 | RMC se conecta a Persistencia |

Con `ufw` (Ubuntu):

```bash
# en M2
sudo ufw allow 5555:5557/tcp

# en M3
sudo ufw allow 5560/tcp
```

M3 además necesita salida a internet para llegar a Supabase (puerto 5432).

---

## 7. Arrancar el sistema

El orden: primero M3, luego M2 y al final M1.

### 7.1 M3: Persistencia

```bash
./scripts/machine3_storage.sh
```

El script carga el `.env`, se conecta a la base, crea las tablas faltantes y reinicia la base: borra los datos existentes y los reemplaza por los eventos de `data/test_case.json`.

Para usar otro estado inicial, pasar las opciones a continuación del script

```
./scripts/machine3_storage.sh -generar                    # 50 eventos, 20 con dos fechas, 100 asientos cada una
./scripts/machine3_storage.sh -inicial data/otro_caso.json  # caso de prueba propio, mismo formato que test_case.json
```

Salida esperada:

```
persistencia 18:33:47.830434 base reiniciada con 2 eventos
persistencia 18:33:47.830893 listo, escuchando en tcp://*:5560
estado inicial de la base
E1 ACM proyectos evento
   - 14 de noviembre: 100 de 100 asientos libres
E2 AESS charla
   - 21 de noviembre: 100 de 100 asientos libres
en total quedan 200 de 200 asientos libres
todavia no hay reservas
```

Deje esta terminal abierta: aquí se ve cómo va quedando la base después de cada reserva.

**Importante:** cada vez que se ejecuta `machine3_storage.sh`, la base se borra y vuelve al estado inicial.

### 7.2 M2: Gestor de Transacciones, RMC y Cancelaciones

```bash
IP_ALMACEN=<IP_M3> ./scripts/machine2_business.sh
```

Los tres procesos arrancan en la misma terminal y cada línea indica cuál habla:

```
GT 18:33:49.775166 listo, los clientes entran por tcp://*:5555, rmc por tcp://*:5556 y cancelaciones por tcp://*:5557
RMC-1 18:33:50.282030 listo, recibe del gestor en tcp://localhost:5556 y habla con persistencia en tcp://<IP_M3>:5560
cancelaciones 18:33:50.282045 listo, conectado al gestor en tcp://localhost:5557
```

Si prefiere ver cada proceso en su propia terminal, arranque estos tres por separado en lugar del script:

```bash
./bin/manager
./bin/rmc -gt tcp://localhost:5556 -persistencia tcp://<IP_M3>:5560
./bin/cancellations -gt tcp://localhost:5557
```

### 7.3 M1: comprobar la conexión

```bash
./bin/client consultar -gt tcp://<IP_M2>:5555
```

Si responde con la lista de eventos, todo está conectado:

```
Cliente1 consulto los eventos de 2026-11, exito: se encontraron 2 eventos en 2026-11 (4.008ms, lo atendio RMC-1)
E1 ACM proyectos evento
   - 14 de noviembre: 100 de 100 asientos libres
...
```

Para no escribir `-gt` en cada comando:

```bash
export SRB_GT=tcp://<IP_M2>:5555
```

---

## 8. Uso del cliente

| Comando | Para qué | Opciones |
|---|---|---|
| `./bin/client consultar` | Ver los eventos de un mes | `-mes 2026-11`, `-id Cliente1` |
| `./bin/client reservar` | Reservar asientos | `-evento E1`, `-ocurrencia 1` (fecha del evento: 1 o 2), `-asientos 5`, `-id Cliente1` |
| `./bin/client concurrente` | Varios clientes reservando el mismo evento a la vez | `-evento E2`, `-asientos 30,25,40,20,15` (un cliente por número) |

Todos aceptan `-gt tcp://<IP_M2>:5555` (o la variable `SRB_GT`) y `-timeout 5s`. Con `-h` se ve la ayuda de cada comando, por ejemplo `./bin/client reservar -h`.

Ejemplos:

```bash
./bin/client consultar -mes 2026-11
./bin/client reservar -id Ana -evento E1 -asientos 2
./bin/client concurrente -evento E2 -asientos 50,50,50
```

Las solicitudes mal formadas las rechaza el GT antes de llegar a RMC:

```
$ ./bin/client reservar -id Ana -evento E1 -asientos 0
Ana pidio 0 asientos de E1 en la fecha 1, fallo: solicitud rechazada, hay que pedir por lo menos 1 asiento

$ ./bin/client consultar -mes noviembre
Cliente1 consulto los eventos de noviembre, fallo: solicitud rechazada, el mes "noviembre" no sirve, escribalo como 2026-11
```

---

## 9. Ver la base de datos

**Desde la terminal de M3**, en cualquier momento y sin detener nada:

```bash
./bin/persistence -ver
```

Muestra los eventos con sus asientos libres y vendidos, y la lista de reservas hechas.

**Desde Supabase:** entre a **Table Editor** y, en el selector de esquema (arriba a la izquierda, dice `public`), elija **`srb`**. Allí están las tablas `events`, `ocurrencias` y `reservas`, y la vista `vista_ocupacion`. También puede usar el **SQL Editor**:

```sql
select * from srb.vista_ocupacion;
select * from srb.reservas order by creada_en;
```

---
