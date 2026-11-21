# imagen de la maquina 2 con el gt, los rmc y cancelaciones, se compila en una etapa y se corre en otra.
# ESCALABILIDAD HORIZONTAL, al correr se pasan IP_ALMACEN con varias ips y RMCS con cuantos rmc lanzar.

# etapa de compilacion con go
FROM golang:1.25-bookworm AS compilar
# instala zeromq para compilar
RUN apt-get update && apt-get install -y --no-install-recommends libzmq3-dev pkg-config && rm -rf /var/lib/apt/lists/*
# carpeta de trabajo
WORKDIR /src
# copia primero las dependencias para aprovechar la cache
COPY go.mod go.sum ./
# descarga las dependencias
RUN go mod download
# copia el resto del codigo
COPY . .
# compila los tres programas de la m2
RUN go build -o /out/ ./cmd/manager ./cmd/rmc ./cmd/cancellations

# etapa final liviana
FROM debian:bookworm-slim
# instala solo la libreria de zeromq para correr
RUN apt-get update && apt-get install -y --no-install-recommends libzmq5 && rm -rf /var/lib/apt/lists/*
# carpeta de trabajo
WORKDIR /app
# trae los programas compilados
COPY --from=compilar /out/ ./bin/
# trae el script que arranca la m2
COPY scripts/machine2_business.sh ./scripts/
# puertos de clientes, rmc y cancelaciones
EXPOSE 5555 5556 5557
# arranca la m2
CMD ["./scripts/machine2_business.sh"]
