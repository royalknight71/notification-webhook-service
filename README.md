# Notification & Webhook Delivery Service

## Overview

This project implements a simple **notification service** that receives events via an HTTP API and delivers them asynchronously to configured webhook URLs. The architecture consists of:

- **Gin HTTP server** exposing health, readiness and a `POST /event` endpoint.
- **PostgreSQL** connection pool (used for readiness checks).
- **RabbitMQ** (AMQP 0‑9‑1) as a durable queue. Events are published to a direct exchange and a background consumer pulls messages, forwards them to the webhook URL, and acknowledges or rejects the message.
- **HMAC‑SHA256 signing** of the webhook payload when `WEBHOOK_SECRET` is set, enabling receivers to verify integrity.

The service is designed for easy local development with Docker Compose and provides comprehensive unit tests for the core components.

---

## Prerequisites

- **Go** 1.22+ (module aware).
- **Docker** and **Docker Compose** for local PostgreSQL and RabbitMQ services.
- (Optional) `make` or a terminal to run the provided commands.

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | HTTP server listen port. | `:8080` (overridden by `config.GetPort()` which prefixes a colon if missing) |
| `DATABASE_URL` | PostgreSQL connection string. | *empty* – must be set for production. |
| `RABBITMQ_URL` | AMQP URL for RabbitMQ. | `amqp://guest:guest@localhost:5672/` |
| `RABBITMQ_EXCHANGE` | Direct exchange name used for publishing. | `webhook_exchange` |
| `RABBITMQ_QUEUE` | Durable queue name the consumer binds to. | `webhook_queue` |
| `RABBITMQ_ROUTING_KEY` | Routing key used when publishing messages. | `webhook_key` |
| `WEBHOOK_SECRET` | Secret used to compute an `X-Signature: sha256=<hex>` header for outgoing webhook requests. | *empty* (signing disabled) |

These defaults are reflected in `.env.example`.

---

## Local Setup with Docker Compose

A minimal `docker-compose.yml` is provided that runs PostgreSQL and RabbitMQ:

```yaml
version: "3.8"
services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
      POSTGRES_DB: notification_service
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "user"]
      interval: 5s
      timeout: 5s
      retries: 5

  rabbitmq:
    image: rabbitmq:3-management
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    ports:
      - "5672:5672"   # AMQP protocol
      - "15672:15672" # Management UI
    healthcheck:
      test: ["CMD", "rabbitmqctl", "status"]
      interval: 5s
      timeout: 5s
      retries: 5
```

Start the services:

```bash
docker compose up -d
```

You can verify they are running via `docker compose ps` or by visiting the RabbitMQ UI at <http://localhost:15672> (guest/guest).

---

## Running the Go Server

1. Copy `.env.example` to `.env` and fill in any required values (e.g., `DATABASE_URL`).
2. Load the variables (or rely on your shell):
   ```bash
   export $(cat .env | xargs)
   ```
3. Build and run:
   ```bash
   go run ./cmd/server
   ```
   The server will listen on the port defined by `PORT` (default `8080`).

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Always returns `200 OK`. Used by orchestration tools. |
| `GET` | `/ready` | Returns `200 OK` only when the PostgreSQL pool can ping the DB; otherwise `503`. |
| `POST` | `/event` | Accepts a JSON payload, assigns a UUID, publishes the event to RabbitMQ, and responds with the generated `event_id`. |

### `POST /event` request format

```json
{
  "target_url": "https://example.com/webhook",
  "payload": { "message": "hello" }
}
```
- `target_url` – Destination webhook URL.
- `payload` – Arbitrary JSON object that will be forwarded unchanged.

### Successful response

```json
{ "event_id": "<uuid>" }
```
- HTTP status `202 Accepted`.

Error responses:
- `400 Bad Request` – Invalid JSON.
- `503 Service Unavailable` – Publisher is nil (RabbitMQ not available).

---

## RabbitMQ Publishing & Consumer Behaviour

1. **Publishing** – `handler.Event` marshals an envelope `{id, target_url, payload}` and publishes it to the exchange defined by `RABBITMQ_EXCHANGE` with routing key `RABBITMQ_ROUTING_KEY`.
2. **Consumer** – `worker.StartConsumer` consumes messages, unmarshals the envelope, and calls `deliverEvent`.
3. **Webhook Delivery**
   - Sends an HTTP `POST` with `Content-Type: application/json`.
   - If `WEBHOOK_SECRET` is set, computes `X-Signature: sha256=<hex>` where `<hex>` is the HMAC‑SHA256 of the raw payload.
   - Retries up to **5 attempts** with exponential back‑off (100 ms × 2^(attempt‑1) + jitter ≤ 100 ms).
   - On success the message is **ACKed**; after the final failed attempt it is **NACKed** (no requeue). Duplicate `id` values are ignored (idempotent) and also ACKed.
4. **Graceful Shutdown** – A shared `context.Context` is cancelled when the server receives a termination signal, causing the consumer loop to stop and the HTTP server to shut down cleanly.

---

## Testing & Development Commands

```bash
# Run all unit tests
go test ./...

# Run tests with the race detector
go test -race ./...

# Format source files (go fmt)
go fmt ./...

# Build the binary
go build ./cmd/server
```

---

## Known Limitations

- Idempotency is limited to the lifetime of the consumer process (in‑memory `sync.Map`).
- Retry jitter uses a deterministic fallback; production may want a stronger random source.
- No TLS configuration for RabbitMQ or the HTTP server – suitable for local development only.

---

Feel free to extend the service with persistence, richer health checks, or additional endpoints as needed.
