# SRB: Sistema de Reserva de Boletos

Sistema distribuido para consultar eventos y reservar asientos. Está escrito en Go, sus componentes se comunican con ZeroMQ y los datos se guardan en Supabase.

Operaciones disponibles:

- **Consulta**: lista los eventos de un mes con sus asientos libres y vendidos.
- **Reserva**: reserva asientos de un evento en una fecha. Si no alcanzan, la reserva se rechaza y la base queda igual.

Ambas son síncronas: el cliente espera la respuesta.

---

## 1. Cómo está armado

El sistema tiene tres capas y cada una corre en su propia máquina:

| Máquina | Capa | Procesos | Qué hace |
|---|---|---|---|
| **M1** | Consumidores | `client` | Envía consultas y reservas |
| **M2** | Negocio | `manager` (Gestor de Transacciones, GT), `rmc`, `cancellations` | El GT recibe las solicitudes, las valida y se las pasa a RMC. RMC consulta la disponibilidad y confirma la reserva. |
| **M3** | Almacenamiento | `persistence` | Es el único proceso que habla con la base de datos

Cómo se conectan los procesos:

| Origen | Destino | Patrón ZeroMQ | Puerto (quién lo abre) |
|---|---|---|---|
| `client` (M1) | `manager` (M2) | REQ / ROUTER | 5555 en M2 |
| `manager` (M2) | `rmc` (M2) | DEALER / REP | 5556 en M2 |
| `manager` (M2) | `cancellations` (M2) | PUSH / PULL | 5557 en M2 |
| `rmc` (M2) | `persistence` (M3) | REQ / REP | 5560 en M3 |
| `persistence` (M3) | Supabase | PostgreSQL | 5432 en Supabase (por internet) |

Recorrido de una reserva: el cliente envía la solicitud al GT, el GT la valida y se la pasa a RMC, RMC le pregunta a Persistencia cuántos asientos quedan y, si alcanzan, le pide guardarla. La respuesta regresa por el mismo camino.

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
| `data/test_case.json` | Datos del caso principal |
| `scripts/` | Scripts para arrancar cada máquina y correr el caso principal |
| `.env.example` | Plantilla del archivo con la conexión a la base |

---

## 2. Requisitos en todas las maquinas que lo probemos

- **Go 1.25 o superior**
- **libzmq**, **pkg-config** y **gcc**: la librería de ZeroMQ para Go (pebbe/zmq4) se compila contra libzmq
- **git**, para descargar el código

### Ubuntu / Debian

```bash
sudo apt update
sudo apt install -y libzmq3-dev pkg-config build-essential git
```

La versión de Go que trae `apt` suele ser vieja. Es mejor instalarla desde https://go.dev/dl descargando el archivo `goX.Y.Z.linux-amd64.tar.gz` más reciente:



## 3. Descargar y compilar 

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

Cada máquina solo usa algunos de ellos, pero compilar todo en las tres no estorba y evita confusiones.

---

## 4. Preparar la base de datos 


### 4.1 Crear el archivo `.env`

En M3, dentro de la carpeta del proyecto:

```bash
cp .env.example .env
```

Abra `.env` y deje la primera línea así, con la URI copiada:

```bash
DATABASE_URL='postgresql://postgres.abcdefghijk:SuPassword123@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require'
```

Revise tres cosas:

- **Quite los corchetes** de `[YOUR-PASSWORD]`. Solo marcan dónde va la contraseña; no son parte de ella.
- **Agregue `?sslmode=require`** al final.
- Si la contraseña tiene caracteres especiales, escríbalos codificados: `@` como `%40`, `#` como `%23`, `/` como `%2F`, `?` como `%3F`, `&` como `%26`. Lo más simple es usar una contraseña solo con letras y números.

El archivo `.env` contiene la contraseña: no se sube a git (está en `.gitignore`) y no debe compartirse.


### Alternativa sin Supabase: PostgreSQL local

Si M3 no tiene internet o no se quiere usar Supabase, el mismo código funciona con un PostgreSQL local en Docker:

```bash
docker run -d --name srb-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
```

En ese caso la línea del `.env` queda:

```bash
DATABASE_URL='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable'
```

---

## 5. Conectar las máquinas

### 5.1 Averiguar las IP

En cada máquina:

```bash
hostname -I
```

Si ese comando no existe, use `ip -4 addr` y busque la línea `inet` de la tarjeta de red (no la de `lo`).

Anote la IP de **M2** y la de **M3**. En este documento aparecen como `<IP_M2>` y `<IP_M3>`. M1 no necesita que nadie se conecte a ella.

Si son máquinas virtuales (VirtualBox, VMware), configure la red en modo **Adaptador puente** o en una **Red NAT** compartida, para que las tres se vean entre sí. Compruébelo desde M1 y desde M2:

```bash
ping -c 3 <IP_M2>
ping -c 3 <IP_M3>
```

### 5.2 Abrir los puertos

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

M3 además necesita salida a internet para llegar a Supabase (puerto 5432). Normalmente ya está permitida.

---

## 6. Arrancar el sistema

El orden importa: primero M3, luego M2 y al final M1.

### 6.1 M3: Persistencia

```bash
./scripts/machine3_storage.sh
```

El script lee el `.env`, se conecta a la base, crea las tablas si faltan y carga los datos del caso principal (`data/test_case.json`). Debe verse así:

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

### 6.2 M2: Gestor de Transacciones, RMC y Cancelaciones

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

### 6.3 M1: comprobar la conexión

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

## 7. Caso principal

Es el caso de prueba que se presenta en la sustentación. La base arranca con dos eventos, E1 y E2, cada uno con una sola fecha en el mes y 100 asientos. Sobre ellos se hacen estas operaciones:

1. Se muestra el estado inicial de la base.
2. Cliente1 reserva 5 asientos de E1.
3. Cliente2 reserva 3 asientos de E1.
4. Se consulta la base: E1 debe tener 92 libres y 8 vendidos.
5. Cinco clientes reservan asientos de E2 **al mismo tiempo** pidiendo 30, 25, 40, 20 y 15 (130 en total, cuando solo hay 100). A alguno se le debe negar la reserva.
6. Se consulta la base: los asientos de E2 quedaron reservados y nunca se venden más de 100.

### Cómo ejecutarlo

Con M3 y M2 corriendo como en la sección 6, en M1:

```bash
IP_NEGOCIO=<IP_M2> ./scripts/test_case.sh
```

El script muestra el título de cada paso y espera un Enter antes de ejecutarlo.

### Qué se ve en M1

```
1. asi arranca la base
Cliente1 consulto los eventos de 2026-11, exito: se encontraron 2 eventos en 2026-11 (4.008ms, lo atendio RMC-1)
E1 ACM proyectos evento
   - 14 de noviembre: 100 de 100 asientos libres
E2 AESS charla
   - 21 de noviembre: 100 de 100 asientos libres
en total quedan 200 de 200 asientos libres

2. Cliente1 reserva 5 asientos de E1
Cliente1 pidio 5 asientos de E1 en la fecha 1, exito: reserva R0001 lista, 5 asientos de E1 en la fecha 1, quedan 95 libres (7.561ms)

3. Cliente2 reserva 3 asientos de E1
Cliente2 pidio 3 asientos de E1 en la fecha 1, exito: reserva R0002 lista, 3 asientos de E1 en la fecha 1, quedan 92 libres (2.496ms)

4. volvemos a mirar la base, E1 deberia tener 92 libres y 8 vendidos
E1 ACM proyectos evento
   - 14 de noviembre: 92 de 100 asientos libres (8 vendidos)
E2 AESS charla
   - 21 de noviembre: 100 de 100 asientos libres

5. varios clientes reservan E2 al mismo tiempo
salen 5 clientes al mismo tiempo por E2 en la fecha 1, entre todos piden 130 asientos
Concurrente-5 pidio 15 asientos, exito: reserva R0003 lista, 15 asientos de E2 en la fecha 1, quedan 85 libres (11.666ms)
Concurrente-1 pidio 30 asientos, exito: reserva R0004 lista, 30 asientos de E2 en la fecha 1, quedan 55 libres (15.783ms)
Concurrente-2 pidio 25 asientos, exito: reserva R0005 lista, 25 asientos de E2 en la fecha 1, quedan 30 libres (20.327ms)
Concurrente-3 pidio 40 asientos, fallo: no alcanzan los asientos de E2 en la fecha 1, pidio 40 asientos y solo quedan 30 (21.489ms)
Concurrente-4 pidio 20 asientos, exito: reserva R0006 lista, 20 asientos de E2 en la fecha 1, quedan 10 libres (23.526ms)
4 clientes consiguieron puesto, 1 se quedo sin puesto y se vendieron 90 de los 130 asientos pedidos

6. asi queda la base al final
E1 ACM proyectos evento
   - 14 de noviembre: 92 de 100 asientos libres (8 vendidos)
E2 AESS charla
   - 21 de noviembre: 10 de 100 asientos libres (90 vendidos)
en total quedan 102 de 200 asientos libres
```

En el paso 5, quién consigue asientos cambia en cada ejecución, porque depende de qué solicitud llega primero. Lo que no cambia es que al menos un cliente es rechazado y nunca se venden más de 100 asientos.

### Qué se ve mientras tanto en M3

Cada operación que llega a la base y el estado después de cada reserva:

```
persistencia 18:33:51.289903 Cliente1 pide 5 asientos de E1 en la fecha 1 (revisa cupos), exito: E1 en la fecha 1 tiene 100 asientos libres
persistencia 18:33:51.294066 Cliente1 pide 5 asientos de E1 en la fecha 1 (guarda la reserva), exito: reserva R0001 lista, 5 asientos de E1 en la fecha 1, quedan 95 libres
E1 ACM proyectos evento
   - 14 de noviembre: 95 de 100 asientos libres (5 vendidos)
E2 AESS charla
   - 21 de noviembre: 100 de 100 asientos libres
en total quedan 195 de 200 asientos libres
reservas hechas: 1
   - R0001: Cliente1 reservo 5 asientos de E1 para el 14 de noviembre
```

### Qué se ve mientras tanto en M2

Lo que hace cada proceso con cada solicitud:

```
RMC-1 18:33:51.294336 Cliente1 pide 5 asientos de E1 en la fecha 1, exito: reserva R0001 lista, 5 asientos de E1 en la fecha 1, quedan 95 libres
GT 18:33:51.294612 RMC-1 contesto la solicitud Cliente1-5fcb40ae0f9b7020, exito: reserva R0001 lista, 5 asientos de E1 en la fecha 1, quedan 95 libres
```

Para repetir el caso principal desde cero, detenga M3 con Ctrl+C y vuelva a ejecutar `./scripts/machine3_storage.sh`.

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

## 10. Cargar otros datos

Persistencia puede arrancar con otro estado inicial:

```bash
# desde otro archivo
./scripts/machine3_storage.sh -inicial ruta/al/archivo.json

# 50 eventos generados: 20 con dos fechas en el mes, 100 asientos cada uno
./scripts/machine3_storage.sh -generar
```

Formato del archivo (el mismo de `data/test_case.json`):

```json
{
  "eventos": [
    {
      "id_evento": "E1",
      "nombre": "Nombre del evento",
      "ocurrencias": [
        { "id_ocurrencia": 1, "fecha": "2026-11-14", "capacidad_total": 100, "asientos_disponibles": 100, "asientos_vendidos": 0 },
        { "id_ocurrencia": 2, "fecha": "2026-11-28", "capacidad_total": 100, "asientos_disponibles": 100, "asientos_vendidos": 0 }
      ]
    }
  ]
}
```

En cada fecha, `asientos_disponibles` más `asientos_vendidos` debe sumar `capacidad_total`. Si no suma, la base rechaza el archivo.

---

## 11. Probar todo en un solo computador

Para probar antes de repartirlo en tres máquinas, abra tres terminales en el mismo computador y use `localhost` como IP:

```bash
# terminal 1 (hace de M3)
./scripts/machine3_storage.sh

# terminal 2 (hace de M2)
IP_ALMACEN=localhost ./scripts/machine2_business.sh

# terminal 3 (hace de M1)
IP_NEGOCIO=localhost ./scripts/test_case.sh
```

---

## 12. Pruebas automáticas

```bash
go test ./...
```

Las pruebas que usan la base de datos **borran sus datos** antes de correr. Por eso solo se ejecutan si se define `SRB_TEST_DATABASE_URL`, y esa variable debe apuntar a un PostgreSQL desechable, nunca a Supabase:

```bash
docker run -d --rm --name srb-pruebas -e POSTGRES_PASSWORD=postgres -p 5433:5432 postgres:16
SRB_TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable' go test -v ./internal/db/
docker stop srb-pruebas
```

Entre ellas está la prueba de concurrencia: 20 reservas simultáneas de 7 asientos sobre 100 asientos deben dejar exactamente 2 libres.

---
