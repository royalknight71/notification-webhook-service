# Notification & Webhook Delivery Service

An HTTP service that accepts events, stores delivery work in PostgreSQL, publishes through a RabbitMQ transactional outbox, and sends webhook requests asynchronously.

## Requirements and configuration

- Go 1.22+
- Docker Compose for local PostgreSQL and RabbitMQ
- `DATABASE_URL` and `RABBITMQ_URL` are required by the server
- Optional: `PORT` (default `8080`), `RABBITMQ_EXCHANGE` (`webhook_exchange`), `RABBITMQ_QUEUE` (`webhook_queue`), `RABBITMQ_ROUTING_KEY` (`webhook_key`), and `WEBHOOK_SECRET`

Copy `.env.example` to `.env` and export its values in your shell. Start local dependencies with:

```sh
docker compose up -d
```

Run the service with `go run ./cmd/server`. The server applies embedded, versioned database migrations before opening its HTTP listener. A migration failure or unavailable required dependency stops startup. The migration version table makes already applied migrations safe to skip on later starts.

## API

`GET /health` returns `200` when the process is alive. `GET /ready` checks PostgreSQL.

`POST /event` accepts:

```json
{
  "target_url": "https://example.com/webhook",
  "payload": { "message": "hello" }
}
```

The service assigns a UUID and commits the event, pending delivery, and outbox record in one PostgreSQL transaction. `202 Accepted` with `{ "event_id": "..." }` means that transaction committed; RabbitMQ and webhook delivery are asynchronous. Invalid JSON returns `400`; persistence failures return `500`.

### Testing a webhook on the Windows host

When `go run ./cmd/server` runs directly on Windows, its consumer is a host process. For a local test receiver, use `127.0.0.1` as the webhook URL; `host.docker.internal` is intended for containers reaching the host and may not resolve in a host process. PowerShell `HttpListener` can also reject host headers that do not match its registered prefixes.

Start the repository's loopback-only receiver in a separate terminal:

```powershell
go run ./cmd/test-webhook
```

It listens at `http://127.0.0.1:8091/`, accepts POSTs from the local service, and prints the received body bytes and `X-Signature`. Submit an event with that URL as `target_url`. The receiver is a local development aid; it does not change production delivery behavior.

For example, from another PowerShell terminal:

```powershell
$body = '{"target_url":"http://127.0.0.1:8091/","payload":{"message":"local test"}}'
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8081/event' -ContentType 'application/json' -Body $body
```

The response includes the event ID. Check its persisted result with:

```powershell
docker exec notification-webhook-service-postgres-1 psql -U user -d notification_service -c "SELECT state, delivered_at FROM deliveries WHERE event_id = '<event-id>';"
```

## Persistence and delivery

The embedded migration creates `webhook_events`, `deliveries`, and `delivery_outbox`. Stable event IDs are unique. Delivery states are `pending`, `processing`, `delivered`, and `failed`, with persisted attempt count, timestamps, and last error.

The dispatcher claims pending outbox rows with `FOR UPDATE SKIP LOCKED` and a lease, commits that claim, then publishes outside the database transaction. Publishing is persistent and mandatory. The outbox row is marked published only after a positive RabbitMQ publisher confirmation and no unroutable return. A crash or uncertain confirmation leaves the row eligible for retry after its lease expires.

The consumer claims delivery rows with a lease, persists each attempt and its error, and retries webhook calls up to five total attempts with exponential backoff and random jitter. Exhausted deliveries are durably marked `failed`. Retryable processing/database errors are requeued with cancellable exponential backoff capped at five seconds; they are not marked failed when PostgreSQL cannot persist their state. Terminal messages are published to a separate durable dead-letter queue and the original is ACKed only after publisher confirmation. The dead-letter exchange and queue names are derived by appending `.dead-letter` to the configured exchange and queue names; the existing work queue is redeclared unchanged.

Readiness checks PostgreSQL, the consumer, and publisher/dispatcher health. An unusable publisher stops the dispatcher and triggers controlled service shutdown so pending outbox rows can resume after restart.

Delivery is **at least once**, not exactly once. A process can fail after the webhook receiver accepts a request but before PostgreSQL records success; the webhook can then be called again. A RabbitMQ confirmation can also be lost after the broker accepted a publish, causing the outbox to publish a duplicate. The event ID is stable in the webhook request; receivers should use it for deduplication.

The current dispatcher does not automatically reconnect a closed RabbitMQ channel and has no operator-facing outbox error state. If RabbitMQ becomes unavailable after startup and the channel closes, pending outbox records remain safe in PostgreSQL but will not publish until the service is restarted.

## Development checks

```sh
go test ./...
go test -race ./...
go build ./...
go vet ./...
go fmt ./...
docker compose config
```

Integration tests are opt-in. PostgreSQL tests create a unique schema in `TEST_DATABASE_URL`. RabbitMQ tests use uniquely named durable exchanges and queues in `TEST_RABBITMQ_URL`; use a disposable vhost. The end-to-end test requires both variables. Tests skip with a clear message when their required variables are absent. These URLs are separate from the service's `DATABASE_URL` and `RABBITMQ_URL` settings.

```sh
docker compose up -d postgres rabbitmq
go test ./internal/delivery -run TestStorePostgres -v
go test ./internal/queue -run TestRabbitMQ -v
go test ./internal/integration -run TestEndToEnd -v
```

For PowerShell, set `TEST_DATABASE_URL` to a disposable local database (for the Compose defaults: `postgres://user:password@localhost:5432/notification_service?sslmode=disable`) and `TEST_RABBITMQ_URL` to a disposable vhost (for example `amqp://guest:guest@localhost:5672/`) before running the commands. The tests clean up their schemas and uniquely named RabbitMQ resources. Unit tests use fakes and do not require Docker. `docker-compose.yml` starts only PostgreSQL and RabbitMQ; run the Go server separately.
