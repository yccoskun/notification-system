.PHONY: test lint build

test:
	@echo "Running test suite"
	go test -v -race ./...

lint:
	golangci-lint run ./...

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -o bin/api ./cmd/api/main.go
	CGO_ENABLED=0 go build -trimpath -o bin/worker ./cmd/worker/main.go
	CGO_ENABLED=0 go build -trimpath -o bin/sweeper ./cmd/sweeper/main.go