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

`GET /health` is a liveness check and returns `200` while the process is running; it does not query dependencies. `GET /ready` checks worker/publisher health and performs one serialized PostgreSQL ping with a one-second timeout. A failed or overlapping readiness check returns `503` without allowing health probes to occupy the pool concurrently.

`GET /metrics` exposes application metrics in Prometheus text format. It uses a private registry and exposes no event IDs, delivery IDs, URLs, payloads, or secrets. `notification_consumer_running` and `notification_outbox_dispatcher_running` report whether those worker goroutines are running (`1`) or stopped (`0`); use `/ready` for dependency readiness.

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

Readiness checks PostgreSQL, the consumer, and publisher/dispatcher health. An unusable publisher stops the dispatcher and triggers controlled service shutdown so pending outbox rows can resume after restart. Application logs use JSON fields for event IDs, delivery attempts, retry delays, errors, and HTTP status without logging webhook payloads or signature secrets. Request URLs are omitted from transport error summaries.

Delivery is **at least once**, not exactly once. A process can fail after the webhook receiver accepts a request but before PostgreSQL records success; the webhook can then be called again. A RabbitMQ confirmation can also be lost after the broker accepted a publish, causing the outbox to publish a duplicate. The event ID is stable in the webhook request; receivers should use it for deduplication.

The current dispatcher does not automatically reconnect a closed RabbitMQ channel and has no operator-facing outbox error state. If RabbitMQ becomes unavailable after startup and the channel closes, pending outbox records remain safe in PostgreSQL but will not publish until the service is restarted.

### Metrics and Prometheus

The metrics endpoint includes:

- `notification_events_accepted_total` and `notification_events_rejected_total` for API request outcomes. Acceptance is counted after the event/outbox transaction commits.
- `notification_delivery_attempts_total` and `notification_delivery_retries_total` for attempts whose counts were persisted before the webhook call.
- `notification_delivery_outcomes_total` and `notification_delivery_retry_exhausted_total` for durable delivery-state transitions. The `outcome` label on the former is limited to `succeeded` or `failed`.
- `notification_delivery_attempt_duration_seconds` as a histogram of individual webhook attempts.
- `notification_outbox_pending` as a PostgreSQL count of unpublished outbox rows, refreshed every 15 seconds. If a sample fails, the gauge retains its last successful value.
- `notification_consumer_running` and `notification_outbox_dispatcher_running` as worker process gauges. A running gauge means its goroutine is alive; it does not replace readiness checks.

### Local Prometheus and Grafana dashboard

Docker Compose can run a local Prometheus and Grafana alongside the existing PostgreSQL and RabbitMQ services. Prometheus reaches the host process through the Docker Desktop host gateway, not container-local `localhost`. The scrape interval is five seconds, and the Prometheus configuration and Grafana dashboard are provisioned from `monitoring/`.

Start the dependencies and monitoring containers from the repository directory:

```powershell
docker compose up -d postgres rabbitmq prometheus grafana
$env:PORT = '8082'
go run ./cmd/server
```

Set the server's required `DATABASE_URL` and `RABBITMQ_URL` in your local shell as described under Requirements before running it; keep real credentials out of shared command history and documentation.

The example uses port `8082` to avoid collisions with other host services; the checked-in scrape target matches it. The server defaults to `8080`, so if you remove or adjust `PORT`, also update the target in `monitoring/prometheus/prometheus.yml`.

Open the dashboard at `http://localhost:3001/d/notification-webhook/notification-webhook`. Grafana anonymous access is enabled as a Viewer and its port is bound to loopback for local demonstration. Prometheus is available at `http://localhost:9090/targets`; its UI and Grafana are also bound to loopback. The dashboard has panels for accepted/rejected events, attempts/retries, durable delivery outcomes, attempt duration, worker status, and unpublished outbox rows. Panels query only metrics listed above. Initially, counters may be zero until requests or delivery attempts occur; histogram quantiles need observations before they become meaningful.

For API access while the server is running, request `http://localhost:8082/metrics`. The provisioned Prometheus scrape configuration is:

```yaml
scrape_configs:
  - job_name: notification-webhook-service
    metrics_path: /metrics
    static_configs:
      - targets: ["host.docker.internal:8082"]
```

When using a different `PORT`, update `monitoring/prometheus/prometheus.yml` to match it. To stop only the containers started for this dashboard and the service dependencies, run `docker compose stop grafana prometheus rabbitmq postgres`; this stops containers without removing their data volumes. Rising delivery retries or exhausted retries indicate webhook failures; a growing outbox gauge indicates events waiting for RabbitMQ publishing. The Prometheus Go client provides exposition format and concurrency-safe metric collectors.

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

For PowerShell, set `TEST_DATABASE_URL` to a disposable local database (for the Compose defaults: `postgres://user:password@localhost:5433/notification_service?sslmode=disable`) and `TEST_RABBITMQ_URL` to a disposable vhost (for example `amqp://guest:guest@localhost:5672/`) before running the commands. The tests clean up their schemas and uniquely named RabbitMQ resources. Unit tests use fakes and do not require Docker. `docker-compose.yml` starts only PostgreSQL and RabbitMQ; run the Go server separately.

### Shutdown and timeouts

The HTTP server applies read/header/write/idle timeouts. On shutdown it stops accepting requests and drains active HTTP handlers for up to five seconds, then force-closes active connections if needed. It stops outbox publishing, asks the consumer to stop taking new messages, and allows its current delivery up to 30 seconds to finish. If that drain expires, the service context is canceled; any unacknowledged RabbitMQ delivery is recovered by RabbitMQ when the connection closes. PostgreSQL operations have a five-second bound, migrations have a 30-second bound, and final worker stop waits are bounded to three seconds. These are bounded best-effort shutdown guarantees; webhook delivery remains at least once.
