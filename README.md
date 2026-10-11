# SRB: Sistema de Reserva de Boletos

Primera entrega, arquitectura base. Go + ZeroMQ (pebbe/zmq4) + PostgreSQL (Supabase, driver pgx).

Operaciones implementadas: **Consulta** y **Reserva** (síncronas). El servicio de **Cancelaciones** (asíncrono, con notificación al cliente) también está desplegado.

## Requisitos

- Go 1.25 o superior
- libzmq, pkg-config y gcc (pebbe/zmq4 usa CGO)
  - Ubuntu/Debian: `sudo apt install libzmq3-dev pkg-config build-essential`
  - Arch: `sudo pacman -S zeromq pkgconf base-devel`
- PostgreSQL 14 o superior: Supabase en la nube o local (`supabase start`)

## Configuración

```bash
cp .env.example .env    # DATABASE_URL = Supabase → Connect → Session pooler
```

El esquema `srb` (tablas, vista y funciones) se crea solo al arrancar Persistencia, a partir de `internal/db/schema.sql`.

## Compilación (en cada máquina)

```bash
go mod tidy
go build -o bin/ ./cmd/...
go test ./...
```

## Despliegue

| Máquina | Capa | Procesos | Puertos |
|---|---|---|---|
| M1 | Consumidores | `client` | 6000 (notificaciones) |
| M2 | Negocio | `manager` (GT), `rmc`, `cancellations` | 5555 clientes, 5556 RMC, 5557 cancelaciones |
| M3 | Almacenamiento | `persistence` → PostgreSQL | 5560 |

Patrones ZeroMQ:

| Comunicación | Patrón |
|---|---|
| Cliente → GT | REQ → ROUTER (síncrono) |
| GT → RMC | DEALER → REP (síncrono) |
| RMC / Cancelaciones → Persistencia | REQ → REP |
| GT → Cancelaciones | PUSH → PULL (asíncrono) |
| Cancelaciones → Cliente | PUSH → PULL (notificación) |

Orden de arranque:

```bash
# M3: carga data/test_case.json (o pasar -generar para 50 eventos, 20 dobles, 100 asientos)
./scripts/machine3_storage.sh

# M2
IP_ALMACEN=<ip M3> ./scripts/machine2_business.sh

# M1: caso de prueba de la sustentación, paso a paso
IP_NEGOCIO=<ip M2> ./scripts/test_case.sh
```

## Comandos del cliente (M1)

```bash
export SRB_GT=tcp://<ip M2>:5555
./bin/client consultar -mes 2026-11
./bin/client reservar -id Cliente1 -evento E1 -asientos 5
./bin/client concurrente -evento E2 -asientos 30,25,40,20,15
./bin/client cancelar -id Cliente1 -reserva R0001 -ipnotif <ip M1>
./bin/client carga -config tests/scenario1.json
```

## Ver la base de datos

- Terminal (M3): `./bin/persistence -ver`
- Supabase Studio → Table Editor → esquema `srb` → `vista_ocupacion`, `reservas`

## Pruebas de rendimiento

Escenarios del enunciado en `tests/scenario1.json`, `tests/scenario2.json` y `tests/scenario3.json` (3, 5 y 8 clientes, 150 s con 15 s de calentamiento, al menos 20 solicitudes por minuto por cliente).

```bash
# M3: 50 eventos y registro de TPM en la BD
./scripts/machine3_storage.sh -generar -tabla=false -metricas metrics/persistencia_s1.csv

# M1: genera la carga e imprime promedio, desviación estándar, P95 y TPM por operación
./bin/client carga -config tests/scenario1.json
```

Los resultados por solicitud quedan en `metrics/*.csv`.
