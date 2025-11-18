.PHONY: help start stop kafka services order payment monitoring clean test logs topics

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

start: kafka services ## Start everything (Kafka + all services)

kafka: ## Start Kafka and monitoring stack
	@echo "🚀 Starting Kafka and monitoring..."
	docker-compose up -d
	@echo "⏳ Waiting for Kafka to be ready..."
	@sleep 30
	@echo "✅ Kafka is ready at localhost:9092"
	@echo "📊 Kafka UI available at http://localhost:8090"
	@echo "📈 Prometheus available at http://localhost:9090"
	@echo "📊 Grafana available at http://localhost:3000 (admin/admin)"

stop: ## Stop all services and Kafka
	@echo "🛑 Stopping services..."
	@pkill -f "order-service" || true
	@pkill -f "payment-service" || true
	@echo "🛑 Stopping Kafka..."
	docker-compose down

kafka-only: ## Start only Kafka (no monitoring)
	docker-compose up -d kafka kafka-ui

services: order payment ## Start all microservices

order: ## Start Order Service
	@echo "🚀 Starting Order Service..."
	@cd order-service && go run main.go &
	@echo "✅ Order Service started on http://localhost:8080"

payment: ## Start Payment Service
	@echo "🚀 Starting Payment Service..."
	@cd payment-service && go run main.go &
	@echo "✅ Payment Service started on http://localhost:8081"

monitoring: ## Open monitoring dashboards
	@echo "📊 Opening monitoring dashboards..."
	@open http://localhost:8090 || xdg-open http://localhost:8090 || echo "Kafka UI: http://localhost:8090"
	@open http://localhost:9090 || xdg-open http://localhost:9090 || echo "Prometheus: http://localhost:9090"
	@open http://localhost:3000 || xdg-open http://localhost:3000 || echo "Grafana: http://localhost:3000"

health: ## Check health of all services
	@echo "🏥 Checking service health..."
	@echo "\nOrder Service:"
	@curl -s http://localhost:8080/health | jq . || echo "❌ Order Service not responding"
	@echo "\nPayment Service:"
	@curl -s http://localhost:8081/health | jq . || echo "❌ Payment Service not responding"

stats: ## Show statistics from all services
	@echo "📊 Service Statistics..."
	@echo "\nOrder Service Stats:"
	@curl -s http://localhost:8080/stats | jq . || echo "❌ Order Service not responding"
	@echo "\nPayment Service Circuit Breaker:"
	@curl -s http://localhost:8081/circuit-breaker | jq . || echo "❌ Payment Service not responding"

test: ## Send test order
	@echo "📦 Sending test order..."
	@echo '{"event_type":"created","order_id":"ORD-$(shell date +%s)","order":{"order_id":"ORD-$(shell date +%s)","customer_id":"CUST-123","status":"pending","items":[{"product_id":"PROD-001","quantity":2,"price":29.99}],"total_amount":59.98,"created_at":"$(shell date -u +%Y-%m-%dT%H:%M:%SZ)","updated_at":"$(shell date -u +%Y-%m-%dT%H:%M:%SZ)"},"timestamp":"$(shell date -u +%Y-%m-%dT%H:%M:%SZ)"}' | \
	  docker-compose exec -T kafka kafka-console-producer --bootstrap-server localhost:9092 --topic order-events
	@echo "✅ Test order sent!"

test-batch: ## Send 10 test orders
	@echo "📦 Sending 10 test orders..."
	@for i in {1..10}; do \
	  echo '{"event_type":"created","order_id":"ORD-'$$i'-$(shell date +%s)","order":{"order_id":"ORD-'$$i'-$(shell date +%s)","customer_id":"CUST-123","status":"pending","items":[{"product_id":"PROD-001","quantity":2,"price":29.99}],"total_amount":59.98,"created_at":"$(shell date -u +%Y-%m-%dT%H:%M:%SZ)","updated_at":"$(shell date -u +%Y-%m-%dT%H:%M:%SZ)"},"timestamp":"$(shell date -u +%Y-%m-%dT%H:%M:%SZ)"}' | \
	    docker-compose exec -T kafka kafka-console-producer --bootstrap-server localhost:9092 --topic order-events; \
	  sleep 0.5; \
	done
	@echo "✅ 10 test orders sent!"

topics: ## List all Kafka topics
	@echo "📋 Kafka Topics:"
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

logs: ## Show logs from all services
	@echo "📜 Service Logs (press Ctrl+C to stop)..."
	@docker-compose logs -f kafka

logs-order: ## Show Order Service logs
	@tail -f order-service/logs/*.log 2>/dev/null || echo "No Order Service logs found"

logs-payment: ## Show Payment Service logs
	@tail -f payment-service/logs/*.log 2>/dev/null || echo "No Payment Service logs found"

watch-orders: ## Watch order-events topic
	@echo "👀 Watching order-events topic (press Ctrl+C to stop)..."
	@docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic order-events --from-beginning

watch-payments: ## Watch payment-results topic
	@echo "👀 Watching payment-results topic (press Ctrl+C to stop)..."
	@docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic payment-results --from-beginning

clean: stop ## Stop everything and clean up data
	@echo "🧹 Cleaning up..."
	docker-compose down -v
	rm -rf order-service/data payment-service/data
	@echo "✅ Cleanup complete"

build: ## Build all services
	@echo "🔨 Building Order Service..."
	@cd order-service && go build -o bin/order-service main.go
	@echo "🔨 Building Payment Service..."
	@cd payment-service && go build -o bin/payment-service main.go
	@echo "✅ Build complete"

install-deps: ## Install Go dependencies
	@echo "📦 Installing dependencies..."
	@cd order-service && go mod download
	@cd payment-service && go mod download
	@echo "✅ Dependencies installed"

# Quick demo
demo: start test ## Start everything and send a test order
	@echo ""
	@echo "🎉 Demo is running!"
	@echo ""
	@echo "📊 Monitor at:"
	@echo "  - Order Service: http://localhost:8080/stats"
	@echo "  - Payment Service: http://localhost:8081/health"
	@echo "  - Kafka UI: http://localhost:8090"
	@echo "  - Grafana: http://localhost:3000"
	@echo ""
	@echo "📝 Useful commands:"
	@echo "  make test        - Send another test order"
	@echo "  make test-batch  - Send 10 test orders"
	@echo "  make health      - Check service health"
	@echo "  make stats       - View statistics"
	@echo "  make watch-orders - Watch order events"
	@echo ""
