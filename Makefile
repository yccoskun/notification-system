.PHONY: test lint build

test:
	@echo "Running test suite"
	go test -v -race ./...

lint:
	golangci-lint run ./...

build:
	go build -o bin/api ./cmd/api/main.go
	go build -o bin/worker ./cmd/worker/main.go
	go build -o bin/sweeper ./cmd/sweeper/main.go