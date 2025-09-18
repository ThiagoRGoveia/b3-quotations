# B3 Quotations Ingestion and API

This project is a complete system for ingesting, storing, and serving B3 (Brasil, Bolsa, Balcão) historical quotation data. It is fully containerized using Docker and managed with a simple `Makefile` for easy setup and operation.

---

## Prerequisites

Before you begin, ensure you have the following installed:
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

---

## Getting Started

Follow these steps to get the application up and running:

### 1. Set up Environment Variables

The project uses an `env-example` file as a template. A `.env` file will be created from this template automatically when you run the `start` or `setup` commands. You can modify `env-example` before running if needed.

### 2. Build and Start the Services

Run the following command to build the Docker images and start the `postgres` and `api` services:

```bash
make start
```

This will start the database and the main API in the background.

### 3. Set up the Database

After starting the services, run the setup script to create the necessary tables in the database:

```bash
make setup
```

### 4. Ingest Data

Place your B3 data files into the `files/` directory. Then, run the ingestion command:

NOTICE: This software assumes the files are in CSV format, already uncompressed, places as is from the [B3 website](https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/cotacoes/cotacoes/).

```bash
make ingest
```

This will start the data ingestion service, which will process the files and load them into the database. You can also specify a different directory:

```bash
make ingest FILES_PATH=./path/to/your/files
```

Your system is now set up and populated with data. The API is running and ready to accept requests on `http://localhost:8080`.

Example request:

```bash
curl --location 'http://localhost:8080/tickers/ETRU25?data_inicio=2025-09-05'
```
---

## Usage (Makefile Commands)

All common tasks are managed via the `Makefile`.

| Command      | Description                                                                                                                             |
|--------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `make start`   | Builds Docker images (if not already built) and starts the `api` and `postgres` services in detached mode.                                |
| `make stop`    | Stops all running Docker containers defined in the `docker-compose.yml` file.                                                           |
| `make setup`   | Ensures the services are running, then executes the database setup script in a temporary container to create the required tables.       |
| `make ingest`  | Runs the data ingestion script in a temporary container. Defaults to processing files in the `./files` directory.                       |
| `make clean-db`| Stops all services and **permanently deletes the database volume**. This is useful for a complete reset of the database environment. |

---

## Project Structure

```
├── cmd/                    # Main applications
│   ├── api/                # API server source code
│   ├── data_ingestion/     # Data ingestion script source code
│   └── setup/              # Database setup script source code
├── internal/               # Internal Go packages (business logic, database, etc.)
├── files/                  # Default directory for placing data files for ingestion
├── .env                    # Environment variables file (auto-generated from env-example)
├── env-example             # Template for environment variables
├── Dockerfile              # Multi-stage Dockerfile for building Go applications
├── docker-compose.yml      # Defines and orchestrates the application services
├── go.mod                  # Go module definition
├── Makefile                # Command-line interface for managing the project
```

---

## Environment Variables

The `env-example` file contains the configuration variables for the application. When you run `make start` or `make setup`, a `.env` file is created from this template.

- `DATABASE_URL`: The connection string for the Go applications to connect to the database.
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`: Used by the `postgres` container for its initial setup.
- `NUM_PARSER_WORKERS`, `NUM_DB_WORKERS_PER_DATE`, `RESULTS_CHANNEL_SIZE`, `DB_BATCH_SIZE`: Configuration parameters for the data ingestion service performance.