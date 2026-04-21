# Webhook Dispatcher

![Go](https://img.shields.io/badge/go-%2300ADD8.svg?style=flat&logo=go&logoColor=white)
![MongoDB](https://img.shields.io/badge/mongodb-%2347A248.svg?style=flat&logo=mongodb&logoColor=white)
![Kafka](https://img.shields.io/badge/kafka-%23231F20.svg?style=flat&logo=apachekafka&logoColor=white)
![Prometheus](https://img.shields.io/badge/prometheus-%23E6522C.svg?style=flat&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/grafana-%23F46800.svg?style=flat&logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)

A distributed webhook delivery system built in Go as a set of decoupled microservices communicating over Kafka and MongoDB. It features per-webhook circuit breakers and token-bucket rate limiting, exponential backoff with jitter, HMAC-SHA256 signed payloads, a scheduler-driven retry loop with at-least-once delivery semantics, and provisioned Grafana dashboards on top of Prometheus metrics.

## Table of Contents

- [Architecture and Features](#architecture-and-features)
  - [Event Ingestion](#event-ingestion)
  - [Dispatching with Circuit Breaker and Rate Limiter](#dispatching-with-circuit-breaker-and-rate-limiter)
  - [Retry Scheduling](#retry-scheduling)
  - [HMAC-Signed Payloads](#hmac-signed-payloads)
  - [Observability](#observability)
- [Design Decisions and Trade-offs](#design-decisions-and-trade-offs)
  - [Kafka-Driven Fan-Out Between Ingestion and Delivery](#kafka-driven-fan-out-between-ingestion-and-delivery)
  - [Event ID as the Kafka Partition Key](#event-id-as-the-kafka-partition-key)
  - [Per-Webhook Isolation for Breakers and Limiters](#per-webhook-isolation-for-breakers-and-limiters)
  - [Scheduler-Driven Retries](#scheduler-driven-retries)
  - [Exponential Backoff with Jitter](#exponential-backoff-with-jitter)
  - [Permanent vs. Retriable Error Classification](#permanent-vs-retriable-error-classification)
  - [At-Least-Once Delivery](#at-least-once-delivery)
  - [Retention Window as Hard Limit](#retention-window-as-hard-limit)
- [Future Improvements](#future-improvements)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Configuration](#configuration)
  - [Execution](#execution)
- [Access Points](#access-points)
- [Usage Example](#usage-example)
- [API Documentation](#api-documentation)
  - [Core Endpoints](#core-endpoints)
- [Run Tests](#run-tests)

## Architecture and Features

The system is structured as four independent microservices, each shipped as its own container and scaled independently. Services never share memory or database sessions. They coordinate entirely through Kafka topics and the shared MongoDB collections.

| Service    | Role                                                                                        |
| ---------- | ------------------------------------------------------------------------------------------- |
| `webhook`  | HTTP API for webhook CRUD and event ingestion. Persists to MongoDB, produces to Kafka.      |
| `dispatch` | Kafka consumer + worker pool. Delivers events to webhook URLs with resilience patterns.     |
| `schedule` | Periodically polls MongoDB for events due for retry and re-publishes them to Kafka.         |
| `client`   | Demo webhook receiver with `/success`, `/error`, and `/poison` endpoints for local testing. |

### Event Ingestion

The `webhook` service accepts event requests over HTTP, validates them against the target webhook's configuration (active flag and enabled event types), persists the full event to MongoDB, and publishes it to the `events.pending` Kafka topic with the event ID as the partition key.

<details>
  <summary>Click to view the Event Ingestion Sequence Diagram</summary>

```mermaid
sequenceDiagram
  autonumber
  participant Client
  participant API as webhook (API)
  participant DB as MongoDB
  participant Kafka as Kafka (events.pending)

  Client->>API: POST /v1/webhooks/:id/events { event_type, data }

  API->>DB: GET webhook by id
  DB-->>API: Webhook

  API->>API: Validate active + event_type in enabled_events
  API->>API: Build Event (id = "ev_" + uuid)

  API->>DB: INSERT event (status = pending, attempts = [])
  DB-->>API: OK

  API->>Kafka: Produce { key: event.id, value: event }
  Kafka-->>API: ACK

  API-->>Client: 201 Created { id }
```

</details>

### Dispatching with Circuit Breaker and Rate Limiter

The `dispatch` service runs a worker pool (default: 100 workers, 1000-slot buffered channel) consuming from `events.pending`. For each event, it fetches the webhook, builds a signed payload, blocks on the webhook's token bucket, and then executes the HTTP request inside the webhook's circuit breaker. The response drives a four-way status transition: `delivered`, `poison`, `failed` (retry scheduled), or `dead` (retention exceeded).

<details>
  <summary>Click to view the Dispatch Flow Diagram</summary>

```mermaid
flowchart TD
  A[Kafka: events.pending] --> B[Consumer fetches message]
  B --> C{JSON unmarshal ok?}
  C -- No --> POISON_MSG[Mark poison in Mongo]
  POISON_MSG --> COMMIT[Commit Kafka offset]

  C -- Yes --> D[Push to worker pool channel]
  D --> E[Worker picks up job]
  E --> F[Fetch webhook from Mongo]
  F --> G{Webhook found?}
  G -- No --> PERM[Permanent error]

  G -- Yes --> H[Build payload + HMAC-SHA256]
  H --> I[Wait on rate limiter]
  I --> J[Execute inside circuit breaker]
  J --> K[HTTP POST to webhook URL]
  K --> L{Response}

  L -- 2xx --> DELIV[status = delivered]
  L -- 4xx except 429 --> PERM
  L -- 5xx / 429 / network --> RETRY{Next retry within<br/>retention window?}

  RETRY -- Yes --> FAIL[status = failed<br/>set next_retry_at]
  RETRY -- No --> DEAD[status = dead]

  PERM --> POISON[status = poison]

  DELIV --> UPDATE[Update event + append attempt]
  POISON --> UPDATE
  FAIL --> UPDATE
  DEAD --> UPDATE
  UPDATE --> COMMIT
```

</details>

### Retry Scheduling

Failed events are not retried in-process. The `schedule` service polls MongoDB every 5 seconds for events whose `next_retry_at` has elapsed, re-publishes them to the same Kafka topic, and flips their status back to `pending`. This keeps the dispatcher's worker pool free for fresh events and makes the retry backlog fully observable as a MongoDB query.

<details>
  <summary>Click to view the Retry Scheduling Sequence Diagram</summary>

```mermaid
sequenceDiagram
  autonumber
  participant Scheduler as schedule
  participant DB as MongoDB
  participant Kafka as Kafka (events.pending)
  participant Dispatcher as dispatch

  loop Every SCHEDULER_DB_PULL_RATE (default 5s)
    Scheduler->>DB: Find { status: failed, next_retry_at ≤ now }<br/>limit [SCHEDULER_DB_BATCH_SIZE], sort by next_retry_at
    DB-->>Scheduler: Due events

    Scheduler->>Kafka: Produce events (re-enqueue)
    Kafka-->>Scheduler: ACK

    Scheduler->>DB: UPDATE status = pending for published ids
    DB-->>Scheduler: OK
  end

  Dispatcher->>Kafka: Consume message
  Kafka-->>Dispatcher: Event (next attempt)
  Dispatcher->>Dispatcher: Re-enters delivery flow
```

</details>

### HMAC-Signed Payloads

Every outbound HTTP request carries an `X-Signature` header containing the HMAC-SHA256 of the request body, signed with the webhook's `whsec_*` secret (generated at webhook creation time and returned once in the create response). Receivers verify the signature before trusting the payload. There is no session or shared token between dispatcher and receiver.

### Observability

The `webhook` and `dispatch` services expose Prometheus metrics on their service ports. Prometheus scrapes both, and Grafana ships with a dashboard covering ingestion rate, dispatch rate by outcome, delivery success ratio, outcome distribution, and retry scheduling rate.

Current metrics:

- `webhook_events_created_total` (counter): events successfully accepted and published.
- `dispatcher_events_processed_total{outcome}` (counter, labeled): events processed by final outcome (`delivered`, `failed`, `poison`, `dead`).

## Design Decisions and Trade-offs

### Kafka-Driven Fan-Out Between Ingestion and Delivery

**Decision:** The ingestion API does not deliver webhooks synchronously. It writes the event to MongoDB, produces it to Kafka, and returns. A separate consumer service is responsible for all delivery work.

- **Rationale:** Webhook receivers are external and unreliable. If delivery were synchronous, an outage in a single receiver would cascade into request latency and timeout errors for the client calling the ingestion API. Kafka decouples the two completely: ingestion stays fast and predictable regardless of how badly receivers are misbehaving.
- **Trade-off:** Clients learn only that the event was _accepted_, not that it was delivered. Delivery status must be queried separately (via the event record in MongoDB).

### Event ID as the Kafka Partition Key

**Decision:** Produced Kafka messages are keyed by the event ID, not by the webhook ID.

- **Rationale:** Keying by webhook ID would concentrate a high-volume customer's events onto a single partition, turning that partition into a bottleneck while the others sit idle. Keying by event ID spreads load uniformly across partitions regardless of how traffic is distributed between webhooks.
- **Trade-off:** There is no per-webhook ordering guarantee; two events for the same webhook can be consumed by different dispatcher workers in any order. The dispatcher delivers and retries each event independently and receivers are already required to be idempotent by event ID, so there was no meaningful ordering to preserve.

### Per-Webhook Isolation for Breakers and Limiters

**Decision:** Circuit breakers and rate limiters are instantiated _per webhook ID_ and stored in `sync.Map`s inside the dispatcher. A given webhook's state is independent of every other webhook.

- **Rationale:** A single misbehaving receiver should not impact the delivery SLA of any other webhook. Per-webhook state keeps blast radius to one customer's endpoint.
- **Trade-off:** Memory grows linearly with the number of active webhooks, and the maps have no eviction, so long-lived dispatcher processes with high webhook churn will accumulate stale entries. Reasonable at current scale; would need an LRU eviction policy if webhook counts grow into the millions.

### Scheduler-Driven Retries

**Decision:** When delivery fails with a retriable error, the dispatcher writes `status=failed` and `next_retry_at` to MongoDB, commits the Kafka offset, and moves on. A separate `schedule` service polls MongoDB and re-publishes due events to the same Kafka topic.

- **Rationale:** In-process retries would block worker goroutines for minutes or hours at a time. Offloading retries to a poll-and-republish service keeps the worker pool free for fresh work and makes the entire retry backlog a queryable MongoDB collection, which is useful for debugging, replay, and operational visibility.
- **Trade-off:** Retry latency is bounded below by the scheduler's poll interval (default 5s). An event whose `next_retry_at` elapses one second after a poll will wait the full interval before being picked up. For webhook delivery this granularity is fine.

### Exponential Backoff with Jitter

**Decision:** Retry delays follow `base * multiplier^attempt` with ±10% uniform jitter, capped at a `max_delay` (defaults produce roughly 30s → 60s → 120s → ... → 6h).

- **Rationale:** Exponential backoff gives a failing endpoint increasing breathing room as failures accumulate. Jitter avoids a thundering herd where many events scheduled during the same outage all retry simultaneously the moment the endpoint recovers.
- **Trade-off:** The retry schedule is less predictable for operators trying to estimate when an event will next be attempted. The trade is worth it; the alternative is coordinated retry storms that re-break a just-recovered endpoint.

### Permanent vs. Retriable Error Classification

**Decision:** Failures are split into two classes with completely different outcomes. 4xx responses (except 429), unknown webhooks, and marshalling errors are **permanent**: the event becomes `poison` and is never retried. 5xx responses, 429, and network errors are **retriable**: the event becomes `failed` with a scheduled retry.

- **Rationale:** Retrying a 404 or 422 just wastes the receiver's capacity and the dispatcher's, since the receiver is explicitly rejecting the event rather than failing to process it. 429 is the exception: it signals "try again later", which is exactly what the retry loop does.
- **Trade-off:** Receivers that return 4xx for _transient_ conditions (e.g., a briefly-unavailable dependency) will see their events poisoned rather than retried. The convention here matches HTTP semantics (4xx means "don't retry this"), so the fix is on the receiver side to return 5xx or 429 for transient errors.

### At-Least-Once Delivery

**Decision:** The system guarantees at-least-once delivery. The same event may be delivered more than once under crashes, slow commits, or scheduler/dispatcher overlap.

- **Rationale:** Exactly-once delivery across Kafka + HTTP + MongoDB would require distributed transactions or a receiver-side idempotency protocol the sender controls, and neither is realistic for webhooks. At-least-once with a stable `event.id` lets receivers deduplicate cheaply on their side.
- **Trade-off:** Webhook receivers must be idempotent with respect to the `id` field in the payload. A non-idempotent receiver will occasionally process the same event twice.

### Retention Window as Hard Limit

**Decision:** When a retriable failure's next scheduled retry would land outside the event's retention window (default 72h from creation), the event is marked `dead` instead of `failed` and is never retried.

- **Rationale:** Without a cap, an event for a permanently-dead endpoint would retry forever, its backoff asymptoting toward `max_delay`, consuming MongoDB storage and polling cycles indefinitely. The retention window draws a firm line: delivery that can't succeed within 72h is accepted as lost.
- **Trade-off:** An endpoint that comes back online after 72h of downtime will not receive events that fell inside the outage window. For most webhook use cases this is acceptable (stale events are often worse than lost ones), but contexts that require unbounded durability would need a different policy or a dead-letter replay path.

## Future Improvements

- **Transactional outbox for MongoDB and Kafka dual writes:** The webhook ingestion path (`internal/webhook/event.go` `publishEvent`) and the scheduler's retry re-enqueue (`internal/schedule/schedule.go` `processRetries`) both write to MongoDB and then produce to Kafka as two independent operations. If the first write succeeds and the second fails, state diverges: an ingested event can sit in Mongo without a matching Kafka message, or a retry is published without its status being updated. At-least-once delivery absorbs the scheduler case via duplicate processing, but the ingestion case leaves an orphan the system never acts on. Moving to an outbox relay pattern (persist the outbound message to a Mongo `outbox` collection inside the same write, then have a relay publish and mark it sent) would close both windows with a single mechanism.
- **Eviction policy for per-webhook state:** Circuit breakers and rate limiters are held in `sync.Map`s keyed by webhook ID with no TTL or LRU eviction, so the maps only grow. Adding an eviction policy would bound dispatcher memory on long-running processes with high webhook churn.
- **Shared circuit breaker and rate limiter state:** Both live in-process. When the dispatcher is scaled out to N replicas, each one maintains its own independent state: the effective rate limit for a webhook becomes `N × configured rate`, and a breaker tripped on one replica has no effect on the others. Moving counters and tokens to a shared store (Redis, etc.) would let multiple replicas act as a single logical limiter.

## Tech Stack

- **Language:** Go 1.26
- **HTTP:** Gin
- **Message Broker:** Apache Kafka (via [segmentio/kafka-go](https://github.com/segmentio/kafka-go))
- **Database:** MongoDB 8
- **Circuit Breaker:** [sony/gobreaker/v2](https://github.com/sony/gobreaker)
- **Rate Limiting:** [golang.org/x/time/rate](https://pkg.go.dev/golang.org/x/time/rate) (token bucket)
- **Metrics:** Prometheus + Grafana
- **Config:** [caarlos0/env](https://github.com/caarlos0/env) + [godotenv](https://github.com/joho/godotenv)
- **Infrastructure:** Docker and Docker Compose

## Getting Started

### Prerequisites

- Docker Engine
- Docker Compose

### Installation

Clone the repository:

```bash
git clone https://github.com/pedroheing/webhook-dispatcher.git && cd webhook-dispatcher
```

### Configuration

The application is pre-configured for the Docker environment. To change the configuration, edit `docker-compose.yml` or copy `.env.example` to `.env` for local runs.

Default variables:

| Variable                             | Description                                    | Default                                                  |
| ------------------------------------ | ---------------------------------------------- | -------------------------------------------------------- |
| `API_PORT_WEBHOOK`                   | Port the webhook API listens on                | `8081`                                                   |
| `API_PORT_CLIENT`                    | Port the demo client listens on                | `8090`                                                   |
| `METRICS_PORT`                       | Port the dispatcher exposes `/metrics` on      | `8082`                                                   |
| `KAFKA_BROKERS`                      | Comma-separated Kafka broker list              | `kafka:29092`                                            |
| `KAFKA_TOPIC`                        | Topic used for pending events                  | `events.pending`                                         |
| `KAFKA_CONSUMER_GROUP`               | Consumer group for the dispatcher              | `webhook-dispatcher`                                     |
| `MONGO_URI`                          | MongoDB connection string                      | `mongodb://admin:password@mongo:27017/?authSource=admin` |
| `MONGO_DATABASE`                     | MongoDB database name                          | `webhook-dispatcher`                                     |
| `DISPATCHER_WORKERS`                 | Number of delivery workers                     | `100`                                                    |
| `DISPATCHER_BUFFER_SIZE`             | Buffered job channel depth                     | `1000`                                                   |
| `DISPATCHER_RETENTION_WINDOW`        | Max age before a failed event is marked `dead` | `72h`                                                    |
| `DISPATCHER_CB_MAX_REQUESTS`         | Requests allowed while breaker is half-open    | `1`                                                      |
| `DISPATCHER_CB_INTERVAL`             | Breaker closed-state counter reset interval    | `60s`                                                    |
| `DISPATCHER_CB_TIMEOUT`              | Open → half-open transition delay              | `60s`                                                    |
| `DISPATCHER_CB_FAILURES_BEFORE_OPEN` | Consecutive failures that trip the breaker     | `5`                                                      |
| `DISPATCHER_LIMITER_REFILL_RATE`     | Token refill rate per second (per webhook)     | `2`                                                      |
| `DISPATCHER_LIMITER_BUCKET_SIZE`     | Token bucket capacity (per webhook)            | `20`                                                     |
| `DISPATCHER_BACKOFF_BASE`            | Base delay for the first retry                 | `30s`                                                    |
| `DISPATCHER_BACKOFF_MAX_DELAY`       | Upper bound on any single retry delay          | `6h`                                                     |
| `DISPATCHER_BACKOFF_MULTIPLIER`      | Multiplier applied per attempt                 | `2`                                                      |
| `DISPATCHER_HTTP_TIMEOUT`            | Per-request HTTP timeout                       | `30s`                                                    |
| `SCHEDULER_DB_PULL_RATE`             | How often the scheduler polls MongoDB          | `5s`                                                     |
| `SCHEDULER_DB_BATCH_SIZE`            | Max events re-enqueued per poll                | `1000`                                                   |

### Execution

The project is fully containerized. To start the application and all dependent services, run:

```bash
docker compose up -d --build
```

On startup, it will:

1. Build the four service images (`webhook`, `dispatch`, `schedule`, `client`) from the shared [Dockerfile](Dockerfile).
2. Start MongoDB, Kafka, Prometheus, Grafana, and the supporting UIs.
3. Create the `events.pending` topic on first run.
4. Start all four services once Mongo and Kafka are available.

## Access Points

| Service                | URL                             | Credentials / Notes                       |
| ---------------------- | ------------------------------- | ----------------------------------------- |
| **Webhook API**        | `http://localhost:8081`         | Event ingestion + webhook CRUD            |
| **Webhook Metrics**    | `http://localhost:8081/metrics` | Prometheus scrape target                  |
| **Dispatcher Metrics** | `http://localhost:8082/metrics` | Prometheus scrape target                  |
| **Demo Client**        | `http://localhost:8090`         | `/success`, `/error`, `/poison` receivers |
| **Grafana**            | `http://localhost:3000`         | Default login: `admin` / `admin`          |
| **Prometheus**         | `http://localhost:9090`         | -                                         |
| **Kafka UI**           | `http://localhost:8080`         | -                                         |
| **Mongo Express**      | `http://localhost:8083`         | Default login: `admin` / `pass`           |

## Usage Example

The demo `client` container exposes three endpoints (`/success`, `/error`, and `/poison`) that each return a different HTTP status. Pointing a webhook at one of them and triggering an event is enough to exercise every status transition in the system.

> The URL uses the Docker network hostname `client` because the dispatcher reaches the demo receiver through the compose network. From outside Docker the same receiver is available at `http://localhost:8090`.

### 1. Register a webhook against the demo receiver

**Success path.** Receiver returns `200`, event becomes `delivered`:

```bash
curl -X POST http://localhost:8081/v1/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "url": "http://client:8090/success",
    "enabled_events": ["payment.created"]
  }'
```

**Retry path.** Receiver returns `500`, event becomes `failed` and is re-enqueued by the scheduler on each scheduled retry until it either succeeds or exceeds the 72h retention window:

```bash
curl -X POST http://localhost:8081/v1/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "url": "http://client:8090/error",
    "enabled_events": ["payment.created"]
  }'
```

**Poison path.** Receiver returns `404`, event becomes `poison` and is never retried:

```bash
curl -X POST http://localhost:8081/v1/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "url": "http://client:8090/poison",
    "enabled_events": ["payment.created"]
  }'
```

Each response returns the webhook `id` and the `secret` used by the receiver to verify `X-Signature`. Save the `id` for the next step.

### 2. Trigger an event

```bash
curl -X POST http://localhost:8081/v1/webhooks/{webhook_id}/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "payment.created",
    "data": { "test": 123 }
  }'
```

### 3. Observe the outcome

- Watch the Grafana dashboard at `http://localhost:3000`. The event shows up as a tick on _Event Ingestion Rate_ and then lands in _Dispatch Rate by Outcome_ under one of `delivered`, `failed`, or `poison`.
- Inspect the event record in Mongo Express at `http://localhost:8083` (database `webhookDispatcherDb`, collection `events`) to see the full delivery history, including `status`, `attempt_number`, `next_retry_at`, and every `DeliveryAttempt` appended so far.
- Check the demo client's container logs (`docker logs wd-client`) to see the incoming request on the receiver side.

## API Documentation

### Core Endpoints

**Webhooks**

- `POST /v1/webhooks`: Create a new webhook. Response includes the `secret` used for HMAC signing (returned only once).
- `GET /v1/webhooks`: List all webhooks.
- `GET /v1/webhooks/:id`: Retrieve a single webhook.
- `PUT /v1/webhooks/:id`: Update URL, enabled events, or active flag.
- `DELETE /v1/webhooks/:id`: Delete a webhook.

**Events**

- `POST /v1/webhooks/:id/events`: Create and enqueue an event for delivery. Body: `{ "event_type": "...", "data": { ... } }`.

**Receiver Contract**

Outbound requests to registered webhook URLs are `POST`s with:

- `Content-Type: application/json`
- `X-Signature: <hex-encoded HMAC-SHA256 of body, signed with the webhook's secret>`
- Body: `{ "id", "event_type", "data", "created_at" }`

Receivers must verify the signature and deduplicate by `id`.

## Run Tests

```bash
go test ./...
```
