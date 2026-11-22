# SRB: Sistema de Reserva de Boletos

Sistema distribuido de tres capas para consultar eventos y reservar, modificar y cancelar asientos, desarrollado como proyecto del curso Introducción a Sistemas Distribuidos. Los procesos están escritos en Go, se comunican mediante ZeroMQ y el estado se guarda en PostgreSQL (Supabase).

Operaciones:

- **Consulta** (síncrona): lista los eventos de un mes con sus asientos libres y vendidos.
- **Reserva** (síncrona): reserva asientos de un evento en una fecha. Si no alcanzan, se rechaza y la base no cambia.
- **Modificación** (síncrona): cambia el número de asientos de una reserva o la pasa a la otra fecha del evento, conservando los asientos. Si no se puede, se rechaza y la reserva queda como estaba.
- **Cancelación** (asíncrona): el GT confirma de inmediato que la recibió y el resultado real le llega al cliente después como notificación.

La segunda entrega agrega una arquitectura **escalable** (varios RMC, varias persistencias, persistencia multihilo, réplica de lectura) y **tolerante a fallas** (Paranoid Pirate entre GT y RMC, Lazy Pirate en los clientes REQ y Binary Star para el GT).

## Contenido

1. [Tecnologías](#1-tecnologías)
2. [Arquitectura](#2-arquitectura)
3. [Requisitos](#3-requisitos)
4. [Descarga y compilación](#4-descarga-y-compilación)
5. [Configuración de la base de datos](#5-configuración-de-la-base-de-datos)
6. [Conectar las máquinas y abrir puertos](#6-conectar-las-máquinas-y-abrir-puertos)
7. [Arrancar el sistema](#7-arrancar-el-sistema)
8. [Uso del cliente](#8-uso-del-cliente)
9. [Opciones de cada programa](#9-opciones-de-cada-programa)
10. [Escalabilidad: cómo verla](#10-escalabilidad-cómo-verla)
11. [Tolerancia a fallas: cómo demostrarla](#11-tolerancia-a-fallas-cómo-demostrarla)
12. [Métricas de desempeño (tabla 1)](#12-métricas-de-desempeño-tabla-1)
13. [Dónde ver cada cosa](#13-dónde-ver-cada-cosa)
14. [Pruebas automáticas y demos](#14-pruebas-automáticas-y-demos)
15. [Todo en una sola máquina](#15-todo-en-una-sola-máquina)

---

## 1. Tecnologías

- **Go 1.25**: todos los procesos del sistema.
- **ZeroMQ** (librería `pebbe/zmq4`): REQ/ROUTER, ROUTER/DEALER (Paranoid Pirate), REQ/REP con Lazy Pirate, PUSH/PULL, PUB/SUB (Binary Star) y el patrón multithreaded server (ROUTER, DEALER interno y workers).
- **PostgreSQL en Supabase** (driver `pgx`): eventos, ocurrencias, reservas y solicitudes procesadas. El esquema y las funciones atómicas están en `internal/db/schema.sql`.
- **Scripts de bash**: arranque de cada máquina.

---

## 2. Arquitectura

### 2.1 Capas

| Máquina | Capa | Programas | Qué hace |
|---|---|---|---|
| **M1** | Consumidores | `client` | Envía consultas, reservas, modificaciones y cancelaciones. También genera carga y calcula métricas. |
| **M2** | Negocio | `manager` (Gestor de Transacciones, GT), `rmc`, `cancellations` | El GT valida y enruta. RMC atiende lo síncrono. Cancelaciones atiende lo asíncrono y notifica al cliente. |
| **M3** | Almacenamiento | `persistence` | Único proceso que habla con la base de datos. |
| M4 u otra (opcional) | Almacenamiento | `persistence` réplica, `manager` de respaldo | Más capacidad de persistencia y GT de respaldo en otra máquina. |

### 2.2 Comunicación entre procesos

| Origen | Destino | Patrón | Puerto |
|---|---|---|---|
| `client` (M1) | `manager` (M2) | REQ / ROUTER, con Lazy Pirate y failover entre GT | 5555 en M2 (5565 en M2 para el respaldo local) |
| `manager` (M2) | `rmc` | ROUTER / DEALER con heartbeats, Paranoid Pirate | 5556 en M2 (5566 respaldo local) |
| `manager` (M2) | `cancellations` | PUSH / PULL | 5557 en M2 (5567 respaldo local) |
| `manager` primario | `manager` respaldo | PUB / SUB con el estado, Binary Star | 5558 en cada GT (5568 respaldo local) |
| `rmc` y `cancellations` | `persistence` (M3, M4) | REQ / ROUTER, con Lazy Pirate | 5560 en M3 y en cada réplica |
| `cancellations` (M2) | `client` (M1) | PUSH / PULL, notificación de la cancelación | el que diga `-puerto` en M1 (por defecto uno libre) |
| `persistence` | Supabase | PostgreSQL | 5432 en Supabase |

### 2.3 Flujo de cada operación

- **Consulta**: cliente → GT → RMC → persistencia (usa la réplica de lectura si existe) → respuesta por el mismo camino.
- **Reserva**: cliente → GT → RMC. RMC pregunta a persistencia la disponibilidad en tiempo real y, si alcanza, le pide crear la reserva. La función `srb.crear_reserva` revisa y resta los asientos en una sola instrucción, así nunca se vende de más aunque lleguen muchas a la vez.
- **Modificación**: cliente → GT → RMC → persistencia. `srb.modificar_reserva` bloquea la reserva y las fechas del evento y hace el cambio completo o no hace nada.
- **Cancelación**: el cliente abre un PULL donde espera la notificación y manda la solicitud con su dirección. El GT la pasa por PUSH al servicio de cancelaciones y le contesta de inmediato "recibida". Cancelaciones llama a `srb.cancelar_reserva`, que devuelve los asientos y marca la reserva como `CANCELADA` sin borrarla, y le manda el resultado al cliente por PUSH.

Toda solicitud lleva un `IdSolicitud` único. La base lo anota en `srb.solicitudes_procesadas` y si la misma solicitud llega otra vez (por un reintento o porque la repitió otro RMC) devuelve la respuesta guardada sin ejecutarla de nuevo. Eso es lo que permite reintentar sin miedo en todos los patrones de tolerancia a fallas.

### 2.4 Estructura del proyecto

| Ruta | Contenido |
|---|---|
| `cmd/client/main.go` | Cliente (M1): consultar, reservar, concurrente, modificar, cancelar |
| `cmd/client/carga.go` | Cliente (M1): generador de carga `carga` y cálculo de métricas `analizar` |
| `cmd/manager/` | Gestor de Transacciones (M2), Paranoid Pirate, Binary Star y conteo de TPM |
| `cmd/rmc/` | Servicio de reservas, modificaciones y consultas (M2) |
| `cmd/cancellations/` | Servicio de cancelaciones (M2) |
| `cmd/persistence/` | Servicio de persistencia multihilo (M3) |
| `internal/model/` | Solicitud, respuesta, evento, ocurrencia, reserva y validaciones |
| `internal/communication/` | Cliente REQ con timeout, reintentos (Lazy Pirate) y modo failover |
| `internal/db/` | Acceso a PostgreSQL, réplica de lectura y esquema `schema.sql` |
| `data/test_case.json` | Estado inicial por defecto (E1 y E2, una fecha cada uno) |
| `data/custom_case.json` | Casos borde (E1 con dos fechas, taller E4 de 10 cupos, evento de diciembre) |
| `scripts/machine3_storage.sh` | Arranca la persistencia principal (M3) y reinicia la base |
| `scripts/persistence_extra.sh` | Arranca una réplica de persistencia en otra máquina, nunca reinicia la base |
| `scripts/machine2_business.sh` | Arranca GT, RMC y cancelaciones (M2), con o sin GT de respaldo |
| `scripts/gt_respaldo.sh` | Arranca el GT de respaldo en otra máquina |
| `scripts/test_case.sh`, `scripts/custom_cases.sh` | Demos paso a paso desde M1 |
| `metrics/` | Se crea al correr `carga`: un csv por prueba y `resumen.csv` |
| `.env.example` | Plantilla del archivo con la conexión a la base |

---

## 3. Requisitos

- **Go 1.25 o superior**
- **libzmq**, **pkg-config** y **gcc**: `pebbe/zmq4` se compila contra libzmq
- **git**

### Ubuntu / Debian

```bash
sudo apt update
sudo apt install -y libzmq3-dev pkg-config build-essential git
```

---

## 4. Descarga y compilación

En cada máquina:

```bash
git clone https://github.com/Ivan140809/SistemaBoletos.git
cd SistemaBoletos
go mod download
go build -o bin/ ./cmd/...
```

Si recibió el proyecto en un `.zip`, descomprímalo y ejecute los tres últimos comandos dentro de la carpeta. Al terminar `ls bin/` debe mostrar cinco programas: `cancellations`, `client`, `manager`, `persistence` y `rmc`.

---

## 5. Configuración de la base de datos

En M3 (y en cada máquina con una réplica de persistencia):

```bash
cp .env.example .env
```

Abra `.env` y ponga la cadena de conexión de Supabase (en el proyecto: **Connect**, opción **Session pooler**), terminada en `?sslmode=require`:

```bash
DATABASE_URL='postgresql://postgres.abcdefghijk:SuPassword123@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require'
```

Opcional, **réplica de lectura**: si tiene una réplica de solo lectura, agregue `DATABASE_URL_LECTURA` con su cadena. Las consultas de eventos irán a la réplica y todo lo que escribe seguirá yendo a la principal. Si no la define, todo va a la principal.

---

## 6. Conectar las máquinas y abrir puertos

### 6.1 Averiguar las IP

En cada máquina:

```bash
ip -br -4 addr
```

La IP es la de la tarjeta que aparece como `UP` y no es `lo` (por ejemplo `wlan0`, `eth0` o `enp0s3`). `docker0`, `virbr0` y `br-...` son redes virtuales y no sirven. Se anota solo la IP, sin lo que va después de la `/`:

```
lo               UNKNOWN        127.0.0.1/8
wlan0            UP             10.150.103.170/22      (esta, la IP es 10.150.103.170)
docker0          DOWN           172.17.0.1/16
```

En este documento aparecen como `<IP_M1>`, `<IP_M2>`, `<IP_M3>`, `<IP_M4>` y `<IP_RESPALDO>`.

Si son máquinas virtuales use red en modo **Adaptador puente** o una **Red NAT** compartida y compruebe que se ven:

```bash
ping -c 3 <IP_M2>
ping -c 3 <IP_M3>
```

### 6.2 Abrir los puertos

| Máquina | Puertos | Para qué |
|---|---|---|
| M1 | el de `-puerto` en `cancelar`, por ejemplo 6000 | Cancelaciones (M2) le entrega la notificación al cliente |
| M2 | 5555, 5556, 5557, 5558 | Clientes, RMC, cancelaciones y el estado del GT para Binary Star |
| M2, con respaldo local | además 5565 a 5568 | Lo mismo para el GT de respaldo que corre en M2 |
| M3 y cada réplica de persistencia | 5560 (o el `PUERTO` de la réplica) | RMC y cancelaciones se conectan a persistencia |
| Máquina del GT de respaldo | 5555 a 5558 | Clientes, RMC, cancelaciones y el estado del GT |

Con `ufw` (Ubuntu):

```bash
# M1, solo si va a cancelar desde otra maquina
sudo ufw allow 6000/tcp

# M2
sudo ufw allow 5555:5558/tcp
sudo ufw allow 5565:5568/tcp     # solo si usa GT_RESPALDO=local

# M3 y cada replica de persistencia
sudo ufw allow 5560/tcp

# maquina del GT de respaldo
sudo ufw allow 5555:5558/tcp
```

M3 y las réplicas necesitan salida a internet para llegar a Supabase (puerto 5432).

---

## 7. Arrancar el sistema

El orden es siempre: **M3, luego las réplicas de persistencia, luego M2 (y el respaldo), y al final M1**.

### 7.1 M3: persistencia principal

```bash
./scripts/machine3_storage.sh
```

El script carga el `.env`, crea las tablas que falten y **reinicia la base**: borra los datos y carga los eventos de `data/test_case.json`. Para otro estado inicial o más workers, pase las opciones después del script (si pasa opciones, incluya también `-inicial` o `-generar`, si no la base no se reinicia):

```bash
./scripts/machine3_storage.sh -generar                          # 50 eventos, 20 con dos fechas, 100 asientos cada una
./scripts/machine3_storage.sh -inicial data/custom_case.json    # casos borde
./scripts/machine3_storage.sh -generar -workers 8               # ESCALABILIDAD VERTICAL, 8 peticiones en paralelo
```

Para las pruebas de modificación de fecha y de carga use `-generar` o `custom_case.json`, porque los eventos de `test_case.json` tienen una sola fecha.

Salida esperada:

```
persistencia 18:33:47.830434 base reiniciada con 2 eventos
persistencia 18:33:47.830893 listo, escuchando en tcp://*:5560 con 4 workers
estado inicial de la base
E1 ACM proyectos evento
   - 14 de noviembre: 100 de 100 asientos libres
...
```

Deje esta terminal abierta: después de cada reserva, modificación o cancelación se vuelve a imprimir la base, y las reservas canceladas aparecen con `(cancelada)`.

### 7.2 Opcional, réplicas de persistencia (M4 u otra)

```bash
./scripts/persistence_extra.sh -workers 8
PUERTO=5561 ./scripts/persistence_extra.sh      # otra replica en la misma maquina, en el puerto 5561
```

Usa la misma base que M3 y **nunca** la reinicia: si se le pasa `-inicial` o `-generar` se niega a arrancar.

### 7.3 M2: Gestor de Transacciones, RMC y cancelaciones

`IP_ALMACEN` lleva las IP de todas las persistencias separadas por coma. `RMCS` dice cuántas réplicas de RMC lanzar (por defecto 3).

**a) Un solo GT**

```bash
IP_ALMACEN=<IP_M3> ./scripts/machine2_business.sh
IP_ALMACEN=<IP_M3>,<IP_M4> RMCS=5 ./scripts/machine2_business.sh     # 2 persistencias y 5 RMC
```

**b) GT primario y respaldo en M2 (Binary Star local)**

```bash
GT_RESPALDO=local IP_ALMACEN=<IP_M3> ./scripts/machine2_business.sh
```

El primario usa los puertos 5555 a 5558 y el respaldo 5565 a 5568. Los RMC y cancelaciones se conectan a los dos.

**c) GT de respaldo en otra máquina**

```bash
# en la maquina del respaldo, primero
IP_PRIMARIO=<IP_M2> ./scripts/gt_respaldo.sh

# en M2
GT_RESPALDO=<IP_RESPALDO> IP_ALMACEN=<IP_M3> ./scripts/machine2_business.sh
```

Todos los procesos escriben en la misma terminal y cada línea dice quién habla:

```
GT-primario 19:59:09.416921 listo, los clientes entran por tcp://*:5555, rmc por tcp://*:5556 y cancelaciones por tcp://*:5557
GT-respaldo 19:59:09.932028 RMC-2-ba3f48 se registro, hay 1 rmc vivos
GT-primario 19:59:09.934752 RMC-1-f248b1 se registro, hay 2 rmc vivos
GT-primario 19:59:12.419852 este gt pasa a ACTIVO porque arranco y el respaldo no esta activo, hay 3 rmc vivos registrados
RMC-1 19:59:09.93 listo, recibe del gestor en tcp://localhost:5556,tcp://localhost:5566 y habla con persistencia en tcp://<IP_M3>:5560
cancelaciones 19:59:09.93 listo, recibe del gestor en tcp://localhost:5557,tcp://localhost:5567 y habla con persistencia en tcp://<IP_M3>:5560
```

Cada RMC se registra con su nombre más un sufijo aleatorio (`RMC-1-f248b1`), que cambia en cada conexión.

Si prefiere ver cada proceso en su propia terminal, arránquelos a mano:

```bash
./bin/manager
./bin/rmc -id RMC-1 -gt tcp://localhost:5556 -persistencia tcp://<IP_M3>:5560
./bin/rmc -id RMC-2 -gt tcp://localhost:5556 -persistencia tcp://<IP_M3>:5560
./bin/cancellations -gt tcp://localhost:5557 -persistencia tcp://<IP_M3>:5560
```

### 7.4 M1: comprobar la conexión

```bash
./bin/client consultar -gt tcp://<IP_M2>:5555
```

Si responde con los eventos, todo está conectado:

```
Cliente1 consulto los eventos de 2026-11, exito: se encontraron 2 eventos en 2026-11 (4.008ms, lo atendio RMC-1)
```

Para no escribir `-gt` en cada comando, use `SRB_GT`. **Con GT de respaldo hay que darle al cliente las dos direcciones**, si no, no conoce al respaldo y no hay failover:

```bash
export SRB_GT=tcp://<IP_M2>:5555                               # un solo GT
export SRB_GT=tcp://<IP_M2>:5555,tcp://<IP_M2>:5565            # respaldo local en M2
export SRB_GT=tcp://<IP_M2>:5555,tcp://<IP_RESPALDO>:5555      # respaldo en otra maquina
```

---

## 8. Uso del cliente

| Comando | Para qué | Opciones propias |
|---|---|---|
| `consultar` | Eventos de un mes | `-mes 2026-11` |
| `reservar` | Reservar asientos | `-evento E1`, `-ocurrencia 1` (fecha 1 o 2), `-asientos 5` |
| `concurrente` | Varios clientes reservando el mismo evento a la vez | `-evento E2`, `-ocurrencia 1`, `-asientos 30,25,40` (un cliente por número) |
| `modificar` | Cambiar asientos **o** fecha de una reserva | `-reserva R0001` y una de `-asientos 8` (nuevo total) o `-ocurrencia 2` (nueva fecha) |
| `cancelar` | Cancelar y esperar la notificación | `-reserva R0001`, `-puerto 6000`, `-ip <IP_M1>`, `-espera 15s` |
| `carga` | Generar carga y medir (sección 12) | `-escenario`, `-duracion`, `-tasa`, `-etiqueta`, ... |
| `analizar` | Métricas de los csv de carga (sección 12) | `-salida metrics/resumen.csv` |

Todos los comandos salvo `analizar` aceptan `-gt` (o `SRB_GT`), `-id`, `-timeout 5s` y `-reintentos 3`. Con `-h` se ve la ayuda de cada uno, por ejemplo `./bin/client modificar -h`.

### Ejemplos

```bash
./bin/client consultar -mes 2026-11
./bin/client reservar -id Ana -evento E1 -ocurrencia 1 -asientos 4
./bin/client concurrente -evento E2 -asientos 50,50,50

./bin/client modificar -id Ana -reserva R0001 -asientos 6      # de 4 a 6 asientos
./bin/client modificar -id Ana -reserva R0001 -ocurrencia 2    # misma reserva a la fecha 2

./bin/client cancelar -id Ana -reserva R0001 -puerto 6000
```

Salida de una cancelación, primero la confirmación del GT y después la notificación:

```
Ana pide cancelar la reserva R0001, exito: cancelacion de R0001 recibida, el resultado le llega como notificacion a tcp://<IP_M1>:6000 (2.1ms)
esperando la notificacion en tcp://<IP_M1>:6000
notificacion para Ana, exito: reserva R0001 cancelada, se liberaron 6 asientos de E1 en la fecha 2, ahora quedan 20 libres
```

Reglas que se pueden mostrar:

- Una modificación cambia **una sola cosa**: asientos o fecha. Con las dos, el GT la rechaza.
- No se puede cambiar de fecha un evento que tiene una sola fecha, ni pasar a una fecha llena.
- Solo el dueño (`-id`) puede modificar o cancelar su reserva, y una reserva cancelada no se puede modificar ni cancelar otra vez.
- Las solicitudes mal formadas las rechaza el GT antes de llegar a RMC:

```
$ ./bin/client reservar -id Ana -evento E1 -asientos 0
Ana pidio 0 asientos de E1 en la fecha 1, fallo: solicitud rechazada, hay que pedir por lo menos 1 asiento
```

**Cancelar entre máquinas**: la notificación la entrega cancelaciones (M2) conectándose a M1, así que M1 debe dejar entrar ese puerto. Use `-puerto` para fijarlo y abrirlo con `ufw` (sección 6.2). Si M1 tiene varias tarjetas de red, use `-ip <IP_M1>` para decir cuál ve M2.

---

## 9. Opciones de cada programa

Solo las que aportan escalabilidad, tolerancia a fallas o métricas. Las demás se ven con `-h`.

| Programa | Opción | Por defecto | Para qué |
|---|---|---|---|
| `client` | `-reintentos` | 3 | Lazy Pirate: cuántas veces reenviar la misma solicitud si no hay respuesta, pasando al otro GT si hay dos |
| `client` | `-timeout` | 5s | Cuánto esperar cada respuesta |
| `manager` | `-latido` | 1s | Cada cuánto se mandan heartbeats a los RMC y se publica el estado al otro GT |
| `manager` | `-vidas` | 3 | Cuántos heartbeats perdidos antes de dar un RMC (o el otro GT) por caído |
| `manager` | `-espera` | 4s | Cuánto espera una solicitud en cola si no hay ningún RMC vivo |
| `manager` | `-rol` | primario | Binary Star: `primario` o `respaldo` |
| `manager` | `-par` | vacío | Binary Star: dirección PUB del otro GT. Vacío, corre solo y siempre activo |
| `manager` | `-estado` | `tcp://*:5558` | Binary Star: dónde publica este GT su estado |
| `manager` | `-tpm` | vacío | csv donde se guardan las transacciones por minuto, además del log |
| `rmc` | `-gt` | `tcp://localhost:5556` | Uno o dos GT separados por coma, se registra en todos |
| `rmc` | `-persistencia` | `tcp://localhost:5560` | Una o varias persistencias separadas por coma |
| `rmc` | `-reintentos` | 1 | Reintentos hacia persistencia, el reintento puede ir a otra réplica |
| `rmc` | `-latido`, `-vidas` | 1s, 3 | Deben coincidir con los del GT |
| `cancellations` | `-gt` | `tcp://localhost:5557` | Uno o dos GT separados por coma |
| `cancellations` | `-persistencia` | `tcp://localhost:5560` | Una o varias persistencias separadas por coma |
| `persistence` | `-workers` | 4 | Escalabilidad vertical: peticiones atendidas en paralelo |
| `persistence` | `-dsn-lectura` | `DATABASE_URL_LECTURA` | Réplica de solo lectura para las consultas |
| `persistence` | `-ver` | | Muestra la base y se sale, sin detener nada |

---

## 10. Escalabilidad: cómo verla

| Elemento | Cómo se activa | Dónde se ve |
|---|---|---|
| Varias réplicas de RMC (horizontal) | `RMCS=5` en `machine2_business.sh`, o más `./bin/rmc` con otro `-id`, en esta u otra máquina | En el cliente, `lo atendio RMC-2`. En el log del GT, qué RMC contestó cada solicitud |
| Varias persistencias (horizontal) | `persistence_extra.sh` en otra máquina y su IP en `IP_ALMACEN=<IP_M3>,<IP_M4>` | Cada terminal de persistencia muestra las peticiones que atendió |
| Persistencia multihilo (vertical) | `-workers 8` en `machine3_storage.sh` o `persistence_extra.sh` | `listo, escuchando en tcp://*:5560 con 8 workers` |
| Réplica de lectura | `DATABASE_URL_LECTURA` en `.env` | Las consultas no cargan la base principal |
| Sin sobreventa con muchos clientes | Siempre, por la función atómica de la base | `./bin/client concurrente -evento E2 -asientos 30,30,30,30,30`: se venden como máximo los asientos que hay |

Un RMC nuevo se puede sumar en cualquier momento sin reiniciar el GT:

```bash
./bin/rmc -id RMC-9 -gt tcp://<IP_M2>:5556 -persistencia tcp://<IP_M3>:5560
```

---

## 11. Tolerancia a fallas: cómo demostrarla

### 11.1 Qué patrón cubre cada falla

| Falla | Patrón | Qué pasa |
|---|---|---|
| Se cae un RMC | **Paranoid Pirate** (GT ROUTER, RMC DEALER, heartbeats cada segundo) | A los 3 s sin señales el GT lo saca y la solicitud que tenía se le pasa a otro RMC con el mismo id |
| Se cae el GT primario | **Binary Star** (primario y respaldo se publican su estado por PUB/SUB) | El respaldo toma el control cuando el primario lleva 3 s callado y le llega un cliente. El cliente reintenta en el otro GT |
| Se cae o se congela una persistencia | **Lazy Pirate** en el REQ de RMC y cancelaciones | Sin respuesta a tiempo, cierra el socket, abre uno nuevo y reintenta; el reintento cae en otra réplica |
| El GT no contesta al cliente | **Lazy Pirate** en el cliente | Reenvía la misma solicitud hasta `-reintentos` veces |
| La misma solicitud se ejecuta dos veces | **Idempotencia** en la base | `srb.solicitudes_procesadas` devuelve la respuesta guardada, nunca se reserva ni se libera dos veces |

Para encontrar un proceso lanzado por el script, use `pgrep -f`, por ejemplo `pgrep -f '^./bin/rmc -id RMC-2'`.

### 11.2 Demostrar la caída de un RMC

1. Arranque M3 y M2 con varios RMC, por ejemplo `RMCS=3`.
2. En M1 lance carga para que haya tráfico todo el tiempo:
   ```bash
   ./bin/client carga -escenario 2 -duracion 2m -etiqueta falla-rmc
   ```
3. En M2, mientras corre, mate un RMC sin avisarle:
   ```bash
   kill -9 $(pgrep -f '^./bin/rmc -id RMC-2')
   ```
4. En el log de M2 se ve:
   ```
   GT 19:40:13.651975 RMC-2-261400 no dio senales en 3s, se da por caido, quedan 2 rmc vivos
   GT 19:40:13.652022 la solicitud Carga3-bcd37492b570fa70 que atendia RMC-2-261400 se le pasa a otro rmc
   GT 19:40:14.622769 RMC-1 contesto la solicitud Carga3-bcd37492b570fa70, exito: ...
   ```
5. Al final de la carga, la columna `red` (errores de red) debe estar en 0: ningún cliente se enteró.
6. Para sustituir el proceso caído, lance otro RMC; se registra solo:
   ```bash
   ./bin/rmc -id RMC-4 -gt tcp://localhost:5556 -persistencia tcp://<IP_M3>:5560
   ```

### 11.3 Demostrar la caída del GT primario

1. Arranque M3 y luego M2 con respaldo local (o con respaldo en otra máquina, sección 7.3):
   ```bash
   GT_RESPALDO=local IP_ALMACEN=<IP_M3> ./scripts/machine2_business.sh
   ```
   Espere a ver `GT-primario ... este gt pasa a ACTIVO`.
2. En M1 dé al cliente las dos direcciones y lance carga:
   ```bash
   export SRB_GT=tcp://<IP_M2>:5555,tcp://<IP_M2>:5565
   ./bin/client carga -escenario 2 -duracion 2m -etiqueta falla-gt
   ```
3. En M2, mate el primario:
   ```bash
   kill -9 $(pgrep -f '^./bin/manager -rol primario')
   ```
4. En M2 se ve al respaldo tomar el control:
   ```
   GT-respaldo 19:59:29.425134 este gt pasa a ACTIVO porque el otro gt no da senales y llego un cliente, hay 3 rmc vivos registrados
   GT-respaldo 19:59:29.427479 RMC-2 contesto la solicitud Carga4-be865ea3e8df8ab8, exito: ...
   ```
   En M1, los clientes muestran el cambio de GT:
   ```
   Carga4: tcp://<IP_M2>:5555 no contesto en 5s, reintento 1 de 3 en tcp://<IP_M2>:5565
   ```
5. Sustituya el primario arrancándolo otra vez. Ve al respaldo activo y se queda pasivo, sin cortar a nadie:
   ```bash
   ./bin/manager -rol primario -par tcp://localhost:5568
   ```
   ```
   GT-primario 19:59:44.469560 el otro gt esta activo, este primario se queda pasivo
   ```
6. Compruebe en M3 con `./bin/persistence -ver` que ninguna reserva quedó repetida.

**Si se cae toda la máquina 2**: con el respaldo en otra máquina, el GT sobrevive, pero los RMC y cancelaciones de M2 también caen. Para cubrir ese caso, lance en la máquina del respaldo sus propios RMC y un servicio de cancelaciones conectados a los dos GT:

```bash
./bin/rmc -id RMC-B1 -gt tcp://localhost:5556,tcp://<IP_M2>:5556 -persistencia tcp://<IP_M3>:5560
./bin/cancellations -gt tcp://localhost:5557,tcp://<IP_M2>:5557 -persistencia tcp://<IP_M3>:5560
```

### 11.4 Demostrar la caída de una persistencia

Con dos persistencias en `IP_ALMACEN`, congele una (`kill -STOP <pid>`) o mátela mientras hay carga. El RMC muestra `persistencia no contesto (...), reintento 1 de 1` y la otra persistencia atiende. Con `kill -CONT <pid>` vuelve sin repetir nada.

---

## 12. Métricas de desempeño (tabla 1)

### 12.1 Escenarios

`carga` ya trae los escenarios de la tabla 1. Cada cliente hace al menos 20 solicitudes por minuto (`-tasa 20`) con intervalos un poco al azar para que no vayan sincronizados.

| Escenario | Clientes |
|---|---|
| 1 | 2 solo consultan, 1 consulta y reserva |
| 2 | 3 solo consultan, 2 reservan y modifican |
| 3 | 4 solo consultan, 4 reservan y modifican |
| 0 | propio: `-clientes 8 -mezcla consulta=40,reserva=30,modificacion=30` |

### 12.2 Cómo correr las pruebas

Arranque M3 con `-generar` para tener suficientes eventos con dos fechas. Luego, desde M1:

```bash
# arquitectura base: 1 GT, 1 RMC, 1 persistencia (RMCS=1 en M2)
./bin/client carga -escenario 1 -etiqueta base -duracion 2m
./bin/client carga -escenario 2 -etiqueta base -duracion 2m
./bin/client carga -escenario 3 -etiqueta base -duracion 2m

# arquitectura escalable: RMCS=3 y dos persistencias, mismos comandos
./bin/client carga -escenario 1 -etiqueta escalable -duracion 2m
./bin/client carga -escenario 2 -etiqueta escalable -duracion 2m
./bin/client carga -escenario 3 -etiqueta escalable -duracion 2m

# resumen de todas las pruebas
./bin/client analizar
```

Opciones útiles de `carga`: `-duracion` (2 a 3 minutos según el enunciado), `-calentamiento 15s` (no cuenta en las métricas), `-semilla 1` (la misma semilla repite la misma carga), `-tasa 0` (sin pausas, para estresar) y `-csv` (otro archivo de salida).

### 12.3 Qué se obtiene

Al terminar, `carga` imprime el resumen de esa prueba y guarda cada solicitud en `metrics/<etiqueta>-e<escenario>.csv`. `analizar` lee todos esos csv y deja `metrics/resumen.csv` listo para las gráficas:

```
etiqueta   esc  operacion       total  exitos    red   prom (ms)   desv (ms)  p95 (ms)      TPM   TPM ok
prueba     2    consulta           73      73      0        5.41        1.44      7.57     73.3     73.3
prueba     2    modificacion       22      22      0       10.47        1.70     13.13     22.1     22.1
prueba     2    reserva            27      27      0       11.61        1.39     13.55     27.1     27.1
```

- **prom, desv, p95**: tiempo de respuesta promedio, desviación estándar y percentil 95, en milisegundos.
- **TPM** y **TPM ok**: transacciones respondidas y exitosas por minuto.
- **red**: solicitudes que no recibieron respuesta ni con los reintentos.

**Medida en el GT**: con `./bin/manager -tpm metrics/tpm-gt.csv`, el GT deja cada minuto en su log y en ese csv cuántas transacciones de cada tipo procesó:

```
GT 20:20:55.314834 TPM ultimo minuto: consultas 74 (74 exitosas), reservas 29 (29 exitosas), modificaciones 18 (18 exitosas), cancelaciones 0
```

El minuto del GT empieza cuando arranca el GT y no cuando arranca la prueba, así que para la tabla use los números de `analizar`, que miden exactamente el tiempo de la prueba.

---

## 13. Dónde ver cada cosa

| Qué | Dónde |
|---|---|
| Estado de la base después de cada cambio | Terminal de M3 (persistencia principal) |
| Estado de la base en cualquier momento | `./bin/persistence -ver` en M3 |
| Qué RMC atendió cada solicitud | Salida del cliente (`lo atendio RMC-x`) y log del GT |
| RMC que entran, se caen y solicitudes reasignadas | Log del GT: `se registro`, `se da por caido`, `se le pasa a otro rmc` |
| Qué GT está activo | Log del GT: `GT-primario` o `GT-respaldo ... pasa a ACTIVO` o `se queda pasivo` |
| Reintentos del cliente | Salida del cliente: `reintento X de Y en <direccion>` |
| Reintentos hacia persistencia | Log del RMC: `persistencia no contesto (...), reintento 1 de 1` |
| Cancelaciones y notificaciones | Log de cancelaciones y salida del cliente (`notificacion para ...`) |
| Transacciones por minuto en el GT | Log del GT y el csv de `-tpm` |
| Métricas de las pruebas | `metrics/*.csv` y `metrics/resumen.csv` |

**Desde Supabase**: en **Table Editor**, cambie el esquema de `public` a **`srb`**. Allí están `events`, `ocurrencias`, `reservas`, `solicitudes_procesadas` y la vista `vista_ocupacion`. En el **SQL Editor**:

```sql
select * from srb.vista_ocupacion;
select * from srb.reservas order by creada_en;
select * from srb.solicitudes_procesadas order by procesada_en desc limit 20;
```

---

## 14. Pruebas automáticas y demos

Pruebas de la base, contra una base de **pruebas** (sus datos se borran, nunca use la de Supabase):

```bash
docker run -d --rm --name srb-pg-prueba -e POSTGRES_PASSWORD=postgres -p 5433:5432 postgres:16-alpine
SRB_TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable' go test ./...
```

Demos guiadas desde M1, avanzan con Enter:

```bash
IP_NEGOCIO=<IP_M2> ./scripts/test_case.sh        # M3 con el estado por defecto
IP_NEGOCIO=<IP_M2> ./scripts/custom_cases.sh     # M3 con -inicial data/custom_case.json
```

---

## 15. Todo en una sola máquina

Para probar todo sin varias máquinas, use una base local y `localhost`. Cada bloque en su propia terminal:

```bash
# terminal 1, base local de prueba y persistencia
docker run -d --rm --name srb-pg -e POSTGRES_PASSWORD=postgres -p 5433:5432 postgres:16-alpine
./bin/persistence -dsn 'postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable' -generar

# terminal 2, una replica de persistencia en el puerto 5561
./bin/persistence -dsn 'postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable' -bind 'tcp://*:5561'

# terminal 3, GT primario y respaldo, 3 RMC y cancelaciones
GT_RESPALDO=local IP_ALMACEN=localhost ./scripts/machine2_business.sh
```

`IP_ALMACEN=localhost` solo apunta al 5560; para usar también la réplica del 5561, arranque M2 a mano como en la sección 7.3 con `-persistencia tcp://localhost:5560,tcp://localhost:5561`.

```bash
# terminal 4, cliente
export SRB_GT=tcp://localhost:5555,tcp://localhost:5565
./bin/client consultar
./bin/client reservar -id Ana -evento E1 -ocurrencia 1 -asientos 4
./bin/client modificar -id Ana -reserva R0001 -ocurrencia 2
./bin/client cancelar -id Ana -reserva R0001
./bin/client carga -escenario 2 -duracion 1m -etiqueta local
./bin/client analizar
```

Con eso puede repetir también las demostraciones de la sección 11 matando procesos con `kill -9 $(pgrep -f ...)`.
