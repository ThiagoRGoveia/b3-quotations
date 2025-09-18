
FROM golang:1.25-alpine AS builder

RUN apk update && apk upgrade

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /setup ./cmd/setup/main.go
RUN go build -o /data_ingestion ./cmd/data_ingestion/main.go
RUN go build -o /api ./cmd/api/main.go
FROM alpine:3.21

RUN apk update && apk upgrade

WORKDIR /app

COPY --from=builder /setup .
COPY --from=builder /data_ingestion .
COPY --from=builder /api .

COPY .env .

EXPOSE 8080
