setup:
	@cp env-example .env
	@docker compose up -d
	@echo "Running database setup..."
	@go run cmd/setup/main.go
	@echo "Database setup finished."

clean-db:
	@echo "Stopping services and removing docker volume..."
	@docker-compose -f ./docker-compose.yml down --volumes
	@echo "Docker volume removed."
