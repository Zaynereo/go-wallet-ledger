PATH := /usr/local/go/bin:$(PATH)
export PATH

MYSQL_DSN ?= ledger:ledger@tcp(127.0.0.1:3306)/ledger?parseTime=true&charset=utf8mb4&loc=UTC

.PHONY: build vet test test-race test-integration lint seed run docker

build:
	go build -o bin/ledger ./cmd/ledger
	go build -o bin/loadtest ./cmd/loadtest

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race -count=1 ./...

test-integration:
	MYSQL_DSN='$(MYSQL_DSN)' REQUIRE_INTEGRATION=1 go test -race -count=1 ./...

lint:
	golangci-lint run ./...

seed:
	MYSQL_DSN='$(MYSQL_DSN)' go run ./cmd/ledger seed

run:
	MYSQL_DSN='$(MYSQL_DSN)' HTTP_ADDR=:8080 PUBLISHER=log go run ./cmd/ledger serve

docker:
	docker build -t go-wallet-ledger:dev .
