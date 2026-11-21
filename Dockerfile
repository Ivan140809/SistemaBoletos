# imagen de la maquina 2 con el gt, los rmc y cancelaciones, se compila en una etapa y se corre en otra.
# ESCALABILIDAD HORIZONTAL, al correr se pasan IP_ALMACEN con varias ips y RMCS con cuantos rmc lanzar.

# etapa de compilacion con go
FROM golang:1.25-bookworm AS compilar
RUN apt-get update && apt-get install -y --no-install-recommends libzmq3-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# compila los tres programas de la m2
RUN go build -o /out/ ./cmd/manager ./cmd/rmc ./cmd/cancellations

# etapa final liviana
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libzmq5 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=compilar /out/ ./bin/
COPY scripts/machine2_business.sh ./scripts/
EXPOSE 5555 5556 5557
CMD ["./scripts/machine2_business.sh"]
