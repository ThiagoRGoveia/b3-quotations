.PHONY: start stop setup ingest clean-db test

# Default path for data ingestion if not provided
FILES_PATH ?= files

start:
	@echo "Starting services..."
	@cp -n env-example .env || true
	@docker compose up -d --build api postgres
	@echo "Services started."

stop:
	@echo "Stopping services..."
	@docker compose down
	@echo "Services stopped."

setup: start
	@echo "Running database setup..."
	@docker compose run --rm setup
	@echo "Database setup finished."

ingest:
	@echo "Starting data ingestion from $(FILES_PATH)..."
	@docker compose run --rm data_ingestion ./data_ingestion project/$(FILES_PATH)
	@echo "Data ingestion finished."

clean-db:
	@echo "Stopping services and removing docker volume..."
	@docker compose down --volumes
	@echo "Docker volume removed."

test:
	@echo "Running tests..."
	@docker compose run --rm test
	@echo "Tests finished."
