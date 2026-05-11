# --- STEP 1: Builder ---
# Updated to the latest stable Go 1.26.3
FROM golang:1.26.3-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

# 1. Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# 2. Copy source code
COPY . .

# 3. Build binaries with optimizations
# Go 1.26's compiler has improved inlining for high-throughput apps
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/bin/api ./cmd/api/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/bin/worker ./cmd/worker/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/bin/sweeper ./cmd/sweeper/main.go

# --- STEP 2: API Runner ---
FROM alpine:3.19 AS api
WORKDIR /app
COPY --from=builder /app/bin/api .
COPY --from=builder /app/migrations ./migrations
EXPOSE 8080
ENTRYPOINT ["./api"]

# --- STEP 3: Worker Runner ---
FROM alpine:3.19 AS worker
WORKDIR /app
COPY --from=builder /app/bin/worker .
EXPOSE 8081
ENTRYPOINT ["./worker"]

# --- STEP 4: Sweeper Runner ---
FROM alpine:3.19 AS sweeper
WORKDIR /app
COPY --from=builder /app/bin/sweeper .
ENTRYPOINT ["./sweeper"]