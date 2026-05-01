.PHONY: up down run migrate test

up:
	docker-compose up -d

down:
	docker-compose down

run:
	go run ./cmd/api/...

migrate:
	go run ./cmd/migrate

test:
	go test ./...
