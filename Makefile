.PHONY: help deps test build clean kafka-up kafka-down run-producer run-consumer run-ecommerce run-dlq run-stateful run-view run-join run-batch

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

deps: ## Download dependencies
	go mod download
	go mod tidy

test: ## Run tests
	go test -v -race -coverprofile=coverage.out ./...

coverage: test ## Show test coverage
	go tool cover -html=coverage.out

build: ## Build all examples
	@echo "Building examples..."
	@cd examples/ecommerce && go build -o ecommerce main.go
	@cd examples/producer && go build -o producer main.go
	@cd examples/consumer && go build -o consumer main.go
	@cd examples/dlq && go build -o dlq main.go
	@cd examples/stateful-processor && go build -o stateful-processor main.go
	@cd examples/view && go build -o view main.go
	@cd examples/join && go build -o join main.go
	@cd examples/batch-consumer && go build -o batch-consumer main.go
	@echo "Build complete!"

clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -f examples/ecommerce/ecommerce
	@rm -f examples/producer/producer
	@rm -f examples/consumer/consumer
	@rm -f examples/dlq/dlq
	@rm -f examples/stateful-processor/stateful-processor
	@rm -f examples/view/view
	@rm -f examples/join/join
	@rm -f examples/batch-consumer/batch-consumer
	@rm -f coverage.out
	@echo "Clean complete!"

kafka-up: ## Start Kafka using Docker Compose
	docker-compose up -d
	@echo "Waiting for Kafka to be ready..."
	@sleep 10
	@echo "Kafka is ready! UI available at http://localhost:8080"

kafka-down: ## Stop Kafka
	docker-compose down

kafka-logs: ## Show Kafka logs
	docker-compose logs -f kafka

run-producer: ## Run producer example
	@echo "Running producer example..."
	go run examples/producer/main.go

run-consumer: ## Run consumer example
	@echo "Running consumer example..."
	go run examples/consumer/main.go

run-ecommerce: ## Run full ecommerce example
	@echo "Running ecommerce example..."
	go run examples/ecommerce/main.go

run-dlq: ## Run DLQ example
	@echo "Running DLQ example..."
	go run examples/dlq/main.go

run-stateful: ## Run stateful processor example
	@echo "Running stateful processor example..."
	go run examples/stateful-processor/main.go

run-view: ## Run view example
	@echo "Running view example..."
	go run examples/view/main.go

run-join: ## Run join example
	@echo "Running join example..."
	go run examples/join/main.go

run-batch: ## Run batch consumer example
	@echo "Running batch consumer example..."
	go run examples/batch-consumer/main.go

fmt: ## Format code
	go fmt ./...

lint: ## Run linter
	@which golangci-lint > /dev/null || (echo "golangci-lint not installed" && exit 1)
	golangci-lint run ./...

vet: ## Run go vet
	go vet ./...

mod-update: ## Update dependencies
	go get -u ./...
	go mod tidy

.DEFAULT_GOAL := help
