.PHONY: up down run migrate test test-integration

up:
	docker-compose up -d --wait
	go run ./cmd/api/...

down:
	docker-compose down

run:
	go run ./cmd/api/...

migrate:
	go run ./cmd/migrate

test:
	go test ./...

test-integration:
	INTEGRATION=1 go test ./...
