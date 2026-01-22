.PHONY: up down ps logs test-env build test lint clean

# Docker Compose commands
up:
	docker-compose up -d
	@echo "Waiting for ClickHouse to be ready..."
	@until docker-compose exec -T clickhouse clickhouse-client --query "SELECT 1" 2>/dev/null; do \
		sleep 1; \
	done
	@echo "ClickHouse is ready at http://localhost:8123"

down:
	docker-compose down -v

ps:
	docker-compose ps

logs:
	docker-compose logs -f clickhouse

# Development commands
build:
	cargo build

test:
	cargo test

lint:
	cargo fmt --check
	cargo clippy -- -D warnings

# Integration test environment
test-env: up
	@echo "Test environment ready!"
	@echo "  - ClickHouse: http://localhost:8123"
	@echo "  - Database: testdb"
	@echo "  - Tables: events, events_raw, events_daily_mv"

# Run integration tests (requires ClickHouse running)
test-integration: up
	cargo test --ignored
	$(MAKE) down

# Clean up
clean: down
	cargo clean
