# Notification Service Makefile

.PHONY: all build run test clean docker-up docker-down migrate-up migrate-down topics kafka-ui seed

DATABASE_URL ?= postgres://notification_user:notification_pass@localhost:5432/notification_db?sslmode=disable
MIGRATIONS_DIR ?= migrations

seed:
	go run cmd/seed/main.go

all: test build

build:
	go build -o bin/server cmd/server/main.go

run:
	go run cmd/server/main.go

test:
	go test -v ./...

clean:
	rm -rf bin/

# Docker development stack
docker-up:
	docker compose -f docker-compose.dev.yml up -d

docker-down:
	docker compose -f docker-compose.dev.yml down -v

# Inspect Kafka topics & partition counts
topics:
	docker exec -it notification-kafka-dev /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

# Start optional Kafka UI dashboard on http://localhost:8080
kafka-ui:
	docker compose -f docker-compose.dev.yml --profile ui up -d kafka-ui

# Database migrations via golang-migrate CLI
migrate-up:
	migrate -path $(MIGRATIONS_DIR) -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path $(MIGRATIONS_DIR) -database "$(DATABASE_URL)" down 1
