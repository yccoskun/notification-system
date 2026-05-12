# notification-system
**event-driven, distributed notification engine** written in Go, deployed as three independently scalable processes that share Postgres, RabbitMQ, and Redis. Below is a structured analysis of its architecture, layering, key design patterns, data flows, and trade-offs.

**Three binaries:**

| Binary | Port | Role |
|--------|------|------|
| `api` | `8080` | REST API, WebSocket hub, migrations on startup |
| `worker` | `8081` (metrics only) | RabbitMQ consumer, delivers notifications via webhooks |
| `sweeper` | — | Periodic job that re-publishes overdue `PENDING` notifications |

**Data flow:**

1. Client sends a `POST /api/v1/notifications` request.
2. API writes a `PENDING` record to Postgres. If `send_at` is now (or unset), it immediately publishes the notification ID to RabbitMQ with its priority.
3. Worker consumes the message, loads the record, applies rate limiting and idempotency guards, optionally renders a template, then POSTs to the appropriate webhook provider (email/SMS/push service).
4. On success the worker marks the notification `SENT`. On retriable failure it schedules a backoff retry. On terminal failure it marks it `FAILED`.
5. Status changes are published to a Redis channel (`notification_status_updates`). The API's WebSocket hub is subscribed to this channel and fans out updates to connected WebSocket clients.
6. The sweeper runs every 30 seconds and picks up any `PENDING` notifications whose `send_at` has passed but were never consumed (e.g., after a crash), republishing them to RabbitMQ.

---

## Design Principles

- **At-least-once delivery**: Notifications are persisted before being queued. The sweeper provides a safety net for messages lost between the API publishing and the worker consuming.
- **Optimistic lease via `send_at`**: When the sweeper or worker picks up a `PENDING` notification, it bumps `send_at` forward by 5 minutes. This acts as a processing lease, preventing multiple workers from delivering the same message concurrently without requiring distributed locks.
- **Idempotency at every layer**: The API uses a unique `idempotency_key` column (`ON CONFLICT DO UPDATE SET updated_at = notifications.updated_at RETURNING id`) so duplicate API calls return the same notification ID. The worker uses a Redis `SETNX` guard (`idemp:worker:<key>`) to prevent double-delivery across retries.
- **Priority queuing**: RabbitMQ queue is declared with `x-max-priority: 10`. Higher-priority notifications are consumed and delivered first.
- **Separation of concerns**: Domain interfaces are defined in `internal/domain` and satisfied by `internal/platform/postgres` and `internal/platform/redis`. The service layer (`internal/service`) depends only on domain interfaces, making it trivially testable.
- **Observable by default**: Every process exports Prometheus metrics, emits structured JSON logs (with trace context injected), and participates in distributed tracing via OpenTelemetry.

---

---

## Domain Model

### Notification

```go
type NotificationStatus string  // "PENDING" | "SENT" | "FAILED" | "CANCELLED"
type ChannelType        string  // "SMS" | "EMAIL" | "PUSH"

type Notification struct {
    ID             uuid.UUID
    BatchID        *uuid.UUID         // non-nil for batch-submitted items
    Recipient      string
    Channel        ChannelType
    TemplateID     *uuid.UUID         // optional; triggers template rendering
    Payload        map[string]any     // free-form data / template variables
    Priority       int                // 1–10; 0 → normalised to 5
    Status         NotificationStatus
    IdempotencyKey *string
    RetryCount     int
    LastError      *string
    SendAt         time.Time          // delivery not before this time
    CreatedAt      time.Time
    UpdatedAt      time.Time
}
```

### Template

```go
type Template struct {
    ID      uuid.UUID
    Name    string       // unique, used as logical key
    Channel ChannelType
    Subject *string      // email only
    Body    string       // Go text/template syntax
}
```

---

## Features & Functionality

### Create a Notification

Send a single notification to one recipient on one channel.

```http
POST /api/v1/notifications
Content-Type: application/json

{
  "idempotency_key": "order-123-email-confirm",
  "recipient": "user@example.com",
  "channel": "EMAIL",
  "priority": 8,
  "payload": {
    "subject": "Your order is confirmed",
    "body": "Hello! Order #123 has been received."
  }
}
```

Response `202 Accepted`:

```json
{
  "notification_id": "550e8400-e29b-41d4-a716-446655440000",
  "send_at": "2026-05-12T06:54:00Z"
}
```

The notification is persisted as `PENDING` and immediately queued for delivery (unless `send_at` is in the future).

**Validation rules:**

| Field | Rule |
|-------|------|
| `idempotency_key` | Required, max 255 chars, trimmed |
| `recipient` | Required, max 255 chars |
| `channel` | Required, one of `SMS`, `EMAIL`, `PUSH` |
| `priority` | `0`–`10`; `0` is normalised to `5` |
| `payload` | Max 512 KiB serialised JSON; keys `_rendered_content` and `_rendered_subject` are reserved |
| `send_at` | Must not be more than 366 days in the future |


---

### Batch Submission

Submit up to 1 000 notifications atomically in a single request. All items share one `idempotency_key` for the batch; each item receives its own UUID. The batch UUID is stored in `batch_id` on each row.

```http
POST /api/v1/notifications/batch
Content-Type: application/json

{
  "idempotency_key": "promo-blast-2026-05-12",
  "notifications": [
    { "recipient": "+15550001", "channel": "SMS",   "priority": 3, "payload": { "body": "Flash sale!" } },
    { "recipient": "+15550002", "channel": "SMS",   "priority": 3, "payload": { "body": "Flash sale!" } },
    { "recipient": "a@b.com",   "channel": "EMAIL", "priority": 5, "payload": { "subject": "Sale", "body": "Flash sale!" } }
  ]
}
```

Response `202 Accepted`:

```json
{
  "batch_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "count": 3
}
```

Retrieve batch status:

```http
GET /api/v1/notifications/batch/7c9e6679-7425-40de-944b-e07fc1f90ae7
```

Returns an array of all notification records in the batch.

---

### Scheduled Delivery

Pass `send_at` to defer delivery until a specific time (max 366 days ahead). The API persists the record as `PENDING` but does not publish it to RabbitMQ. The sweeper will detect and publish it once `send_at` has passed.

```json
{
  "idempotency_key": "reminder-abc",
  "recipient": "+15550001",
  "channel": "SMS",
  "payload": { "body": "Your appointment is in 1 hour." },
  "send_at": "2026-05-13T09:00:00Z"
}
```

---

### Priority Queuing

Priority is an integer `1`–`10` (default `5`). RabbitMQ is configured with `x-max-priority: 10`, so higher-priority messages are dequeued and delivered before lower-priority ones. Priority `0` in the request body is silently normalised to `5`.

---

### Idempotency

Submitting the same `idempotency_key` twice is safe. The database uses `ON CONFLICT (idempotency_key) DO UPDATE SET updated_at = notifications.updated_at RETURNING id`, so the second call returns the original `notification_id` without creating a duplicate or re-queuing.

The worker additionally uses a Redis `SETNX` guard keyed as `idemp:worker:<idempotency_key>` to prevent double-delivery in the case of worker restarts mid-flight.

---

### Cancellation

Cancel a `PENDING` notification. Notifications that are already `SENT` or `FAILED` cannot be cancelled.

```http
DELETE /api/v1/notifications/550e8400-e29b-41d4-a716-446655440000
```

Response `200 OK` on success; `409 Conflict` if the notification is not in `PENDING` state.

The cancellation also publishes a `CANCELLED` status event to the Redis pub/sub channel, which is forwarded to WebSocket clients watching that notification.

---

### Listing & Filtering

```http
GET /api/v1/notifications?status=PENDING&channel=EMAIL&created_from=2026-05-01T00:00:00Z&limit=25&offset=0
```

Query parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `status` | string | Filter by status: `PENDING`, `SENT`, `FAILED`, `CANCELLED` |
| `channel` | string | Filter by channel: `SMS`, `EMAIL`, `PUSH` |
| `created_from` | RFC3339 | Lower bound for `created_at` |
| `created_to` | RFC3339 | Upper bound for `created_at` |
| `limit` | int | Page size, default `50`, max `100` |
| `offset` | int | Pagination offset, default `0` |

Response `200 OK`:

```json
{
  "notifications": [ /* array of notification objects */ ],
  "total": 142,
  "limit": 25,
  "offset": 0
}
```

Results are ordered by `created_at DESC, id DESC`.

---

### Template Rendering

Create a named template in Postgres with a Go `text/template` body. When a notification references a `template_id`, the worker renders the template with the notification's `payload` map as the data context. The rendered output is stored into `payload._rendered_content` (and `payload._rendered_subject` for EMAIL) before calling the provider.

Template body example:

```
Hello {{.name}}, your order {{.order_id}} has shipped!
```

Notification payload:

```json
{ "name": "Alice", "order_id": "ORD-987" }
```

Rendered result: `Hello Alice, your order ORD-987 has shipped!`

---

### Rate Limiting

The worker enforces a per-recipient, per-channel rate limit using a Redis Lua token-bucket script. The key is `ratelimit:<channel>:<recipient>`. If the limit is exceeded, the delivery is deferred by scheduling a retry via `ScheduleRetry`.

---

### Retry with Exponential Backoff

When a delivery attempt fails with a retriable error (5xx from the provider, network error), the worker calls `ScheduleRetry` with a delay computed by:

```
delay = rand(0, min(base * 2^attempt, max))
```

- Base delay: **2 seconds**
- Maximum delay: **1 hour**
- Maximum retries: **5**

After 5 failed attempts the notification is marked `FAILED`.

---

### Circuit Breaker

Each webhook provider is wrapped with a `gobreaker` circuit breaker (one per provider channel). It trips open after ≥10 requests with ≥50% failures. After 30 seconds in the open state, it allows a single probe request. If the probe succeeds, the breaker closes. This prevents cascading failures when a downstream provider is down.

---

### Real-time Status via WebSocket

Connect to the WebSocket endpoint to receive live status updates for all notifications (or a specific one).

```
ws://localhost:8080/api/v1/ws
ws://localhost:8080/api/v1/ws?watch=550e8400-e29b-41d4-a716-446655440000
```

Messages are JSON:

```json
{ "id": "550e8400-e29b-41d4-a716-446655440000", "status": "SENT" }
{ "id": "550e8400-e29b-41d4-a716-446655440000", "status": "FAILED", "detail": "retry_scheduled" }
```

The API process subscribes to the Redis channel `notification_status_updates`. Status-change events published by the worker are relayed to the Redis channel, then fanned out by the in-process `WSHub` to all connected WebSocket clients (optionally filtered by the `watch` query parameter).

Allowed origins are controlled by the `WEBSOCKET_ALLOWED_ORIGINS` environment variable.

---

### Sweeper: Reliable At-Least-Once Delivery

The sweeper is a safety net that runs every **30 seconds**. It queries Postgres for `PENDING` notifications whose `send_at` has passed (using `FOR UPDATE SKIP LOCKED` to avoid contention with the worker) and republishes them to RabbitMQ in batches of up to 100. This handles:

- Notifications whose initial publish was lost (e.g., RabbitMQ was temporarily unreachable).
- Scheduled notifications that have become due.
- Notifications whose worker lease expired (the worker crashed mid-flight).

If re-publishing a notification fails, the sweeper calls `ScheduleRetry` with `time.Now()` so it will be picked up on the next sweep.

---

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness probe — returns `{"status":"UP"}` |
| `GET` | `/metrics` | Prometheus metrics (HTTP) |
| `GET` | `/docs/` | Swagger UI |
| `GET` | `/docs/openapi.yml` | Raw OpenAPI 3 spec |
| `POST` | `/api/v1/notifications` | Create a single notification |
| `GET` | `/api/v1/notifications` | List notifications with filters |
| `POST` | `/api/v1/notifications/batch` | Submit a batch of notifications |
| `GET` | `/api/v1/notifications/batch/:batch_id` | Get all notifications in a batch |
| `GET` | `/api/v1/notifications/:id` | Get a single notification by ID |
| `DELETE` | `/api/v1/notifications/:id` | Cancel a pending notification |
| `GET` | `/api/v1/ws` | WebSocket — real-time status stream |

Every request receives an `X-Request-ID` header in the response (generated if not supplied by the caller) and the same ID is propagated through all log lines and trace spans for that request.

Full interactive documentation is available at `http://localhost:8080/docs/` when the API is running.

---

## Configuration

All configuration is via environment variables. No config files are required at runtime.

| Variable | Required by | Description |
|----------|------------|-------------|
| `DATABASE_URL` | api, worker, sweeper | Postgres DSN, e.g. `postgres://user:pass@localhost:5432/notifs?sslmode=disable` |
| `REDIS_URL` | api, worker | Redis address, e.g. `localhost:6379` |
| `RABBITMQ_URL` | api, worker, sweeper | AMQP URL, e.g. `amqp://guest:guest@localhost:5672/` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | api, worker, sweeper | OTLP gRPC endpoint (optional), e.g. `localhost:4317` |
| `EMAIL_PROVIDER_URL` | worker | Webhook URL for email delivery |
| `SMS_PROVIDER_URL` | worker | Webhook URL for SMS delivery |
| `PUSH_PROVIDER_URL` | worker | Webhook URL for push delivery |
| `WEBSOCKET_ALLOWED_ORIGINS` | api | Comma-separated allowed WebSocket origins; empty or `*` allows all |

Create a `.env` file (gitignored) and source it before running locally, or use the Docker Compose environment blocks.

---

## Observability

### Logging

All processes emit structured JSON logs via `slog`. Each log record includes:

- `service` — `"api"`, `"worker"`, or `"sweeper"`
- `trace_id`, `span_id` — injected from the active OTel span when available
- `request_id` — from the HTTP correlation middleware (`X-Request-ID`)

### Tracing

Distributed tracing via OpenTelemetry. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to a Jaeger (or any OTLP gRPC) endpoint. OTel context is propagated across HTTP (W3C TraceContext + Baggage) and through AMQP message headers, so a single request trace spans the API, RabbitMQ, and worker.

Jaeger UI is available at `http://localhost:16686` when using Docker Compose.

### Metrics

Prometheus metrics are exposed at:

- API: `http://localhost:8080/metrics`
- Worker: `http://localhost:8081/metrics`

Key metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `http_request_duration_seconds` | Histogram | HTTP request latency by method, path, status |
| `http_requests_in_flight` | Gauge | Concurrent HTTP requests |
| `notification_queue_depth` | Gauge | Per-channel, per-status row counts |
| `notifications_ingress_total` | Counter | Notifications accepted by the API |
| `notifications_delivered_total` | Counter | Successful deliveries by the worker |
| `notifications_failed_total` | Counter | Terminal failures by the worker |
| `delivery_duration_seconds` | Histogram | End-to-end delivery latency |
| `rate_limit_hits_total` | Counter | Rate limit rejections |

Prometheus is preconfigured to scrape both endpoints. Grafana can be connected to `http://prometheus:9090`.

---

## Database Schema

### `templates`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `UUID` | `DEFAULT gen_random_uuid()` |
| `name` | `VARCHAR(255)` | Unique |
| `channel` | `VARCHAR(50)` | |
| `subject` | `VARCHAR(255)` | Nullable; used for EMAIL |
| `body` | `TEXT` | Go `text/template` body |
| `created_at` | `TIMESTAMPTZ` | |
| `updated_at` | `TIMESTAMPTZ` | |

### `notifications`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `UUID` | PK |
| `batch_id` | `UUID` | Nullable; groups batch items |
| `recipient` | `VARCHAR(255)` | |
| `channel` | `channel_type` | `SMS`, `EMAIL`, `PUSH` |
| `template_id` | `UUID` | FK → `templates`, nullable |
| `payload` | `JSONB` | |
| `priority` | `SMALLINT` | `CHECK (priority BETWEEN 1 AND 10)`, default `1` |
| `status` | `notification_status` | `PENDING`, `SENT`, `FAILED`, `CANCELLED` |
| `idempotency_key` | `VARCHAR(255)` | Unique, nullable |
| `retry_count` | `INT` | Default `0` |
| `last_error` | `TEXT` | Nullable |
| `send_at` | `TIMESTAMPTZ` | Delivery not before this time; used as processing lease |
| `created_at` | `TIMESTAMPTZ` | |
| `updated_at` | `TIMESTAMPTZ` | |

**Indexes:**

- `idx_notifications_sweeper` — partial index on `(priority DESC, send_at ASC) WHERE status = 'PENDING'` — used by the sweeper and `GetPendingForDelivery`
- `idx_notifications_batch` — partial index for batch lookups
- `idx_notifications_list_created` — for list API sorting
- `idx_notifications_status_created` — for filtered list queries

---

## Setup & Running

### Prerequisites

- **Docker** and **Docker Compose** (for the quick-start path)
- **Go 1.26+** (for local development)
- `golangci-lint` (for linting)

### Quick Start with Docker Compose

1. Copy the example environment file and fill in your values:

```bash
cp .env.example .env   # or create .env manually
```

Minimum `.env` for local development:

```env
POSTGRES_USER=notify
POSTGRES_PASSWORD=secret
POSTGRES_DB=notifications
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
EMAIL_PROVIDER_URL=http://your-email-webhook/send
SMS_PROVIDER_URL=http://your-sms-webhook/send
PUSH_PROVIDER_URL=http://your-push-webhook/send
```

2. Start the full stack:

```bash
docker compose -f deployments/docker-compose.yml up --build
```

This starts: Postgres, RabbitMQ, Redis, the API, the worker, the sweeper, Prometheus, and Jaeger.

3. Verify the API is healthy:

```bash
curl http://localhost:8080/health
# {"status":"UP"}
```

4. Open the Swagger UI: [http://localhost:8080/docs/](http://localhost:8080/docs/)

5. Open Jaeger UI: [http://localhost:16686](http://localhost:16686)

6. Open Prometheus: [http://localhost:9090](http://localhost:9090)

### Build Binaries

```bash
make build
# Produces: bin/api  bin/worker  bin/sweeper
```

---

## Testing

Tests are divided into unit tests (with mock/stub interfaces) and integration tests (using Testcontainers to spin up real Postgres, Redis, and RabbitMQ).

```bash
# Run all tests with race detection
make test

# Run a specific package
go test -v -race ./internal/api/...
go test -v -race ./internal/platform/postgres/...
go test -v -race ./internal/platform/redis/...
go test -v -race ./internal/platform/rabbitmq/...
go test -v -race ./internal/service/...
```

**Test coverage by package:**

| Package | What is tested |
|---------|---------------|
| `internal/api` | Handler logic for scheduling, cancellation, listing, batch; validation rules (per-channel payloads, reserved keys, send_at horizon, priority normalisation); WebSocket message parsing |
| `internal/platform/postgres` | Full CRUD flows via Testcontainers Postgres; batch insert; status transitions; `GetPendingForDelivery` with concurrent workers and `SKIP LOCKED`; idempotency conflict; list filters and pagination |
| `internal/platform/redis` | Token-bucket rate limiter boundary conditions and concurrency; `StatusUpdate` JSON round-trip |
| `internal/platform/rabbitmq` | Publish and consume with OTel trace context propagation (Testcontainers RabbitMQ) |
| `internal/service` | Sweeper retry-on-publish-failure behaviour |

---

## CI/CD

GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push and pull request:

1. `gofmt` format check
2. `go vet`
3. `golangci-lint run` (7-minute timeout, config in `.golangci.yml`)
4. `make test` — all tests with race detector
5. `make build` — verify all three binaries compile

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/gin-gonic/gin` | HTTP router and middleware |
| `github.com/jackc/pgx/v5` | Postgres driver with connection pooling |
| `github.com/redis/go-redis/v9` | Redis client |
| `github.com/rabbitmq/amqp091-go` | RabbitMQ AMQP 0-9-1 client |
| `github.com/golang-migrate/migrate/v4` | SQL migration runner |
| `github.com/google/uuid` | UUID generation and parsing |
| `github.com/gorilla/websocket` | WebSocket server |
| `github.com/prometheus/client_golang` | Prometheus metrics |
| `go.opentelemetry.io/otel` | OTel SDK (tracing) |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` | OTLP gRPC trace exporter |
| `go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin` | Gin OTel middleware |
| `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp` | HTTP client OTel transport |
| `github.com/sony/gobreaker` | Circuit breaker |
| `github.com/stretchr/testify` | Test assertions |
| `github.com/testcontainers/testcontainers-go` | Containerised integration tests |
