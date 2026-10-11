# SRB: Sistema de Reserva de Boletos

Primera entrega, arquitectura base. Go + ZeroMQ (pebbe/zmq4) + PostgreSQL (Supabase, driver pgx).

Operaciones implementadas: **Consulta** y **Reserva** (síncronas). El servicio de **Cancelaciones** forma parte de la arquitectura y se conecta al GT, pero la operación de cancelar se implementa en la segunda entrega. No se implementa tolerancia a fallas.

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
| M1 | Consumidores | `client` | — |
| M2 | Negocio | `manager` (GT), `rmc`, `cancellations` | 5555 clientes, 5556 RMC, 5557 cancelaciones |
| M3 | Almacenamiento | `persistence` → PostgreSQL | 5560 |

Patrones ZeroMQ:

| Comunicación | Patrón |
|---|---|
| Cliente → GT | REQ → ROUTER (síncrono) |
| GT → RMC | DEALER → REP (síncrono) |
| RMC → Persistencia | REQ → REP |
| GT → Cancelaciones | PUSH → PULL (canal asíncrono; sin operaciones en esta entrega) |

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
```

## Ver la base de datos

- Terminal (M3): `./bin/persistence -ver`
- Supabase Studio → Table Editor → esquema `srb` → `vista_ocupacion`, `reservas`
