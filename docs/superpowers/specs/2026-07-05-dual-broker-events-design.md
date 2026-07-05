# Dual-Broker Event Pipeline — Design

**Date:** 2026-07-05
**Status:** Approved (design), pending implementation plan
**Context:** Learning exercise bolted onto the existing `better-auth` Turborepo (NestJS + better-auth + Drizzle/Postgres backend, Next.js web app).

## Goal

When a run advances, publish the same logical event to **two** consumers and let their
divergent behavior teach the RabbitMQ-vs-Kafka contrast:

- **RabbitMQ = smart broker**: push, per-message ack, routing keys, DLQ, *ephemeral* (acked = gone).
- **Kafka = dumb replayable log**: pull, offset-based, partitions, consumer groups, *replayable* (rebuild state from offset 0).

The exercise is considered successful when the learner can demonstrate — with running
code — every item in the Acceptance section, culminating in the stretch goal: rebuild the
Kafka "runs completed per workflow" count by replaying from offset 0, which is
**impossible** against the drained Rabbit queue. That impossibility is the entire point.

## Non-goals

- Production hardening (HA brokers, auth on brokers, exactly-once semantics).
- Replacing or altering existing auth behavior. Auth is only a *secondary event producer*.
- Any UI for the pipeline beyond the broker management UIs and DB inspection.

## Decisions (locked during brainstorming)

| Decision | Choice |
|---|---|
| Event source | **Both**: a new run/workflow domain (primary) **and** auth lifecycle events (secondary), into the same EventBus. |
| Client abstraction | **NestJS microservices transport** (`@nestjs/microservices`) on the Node side — but configured to keep primitives visible (manual ack, routing keys, DLQ). |
| Go Kafka library | **Both** `segmentio/kafka-go` and `IBM/sarama`, behind one interface, selectable by flag — to compare ergonomics. |
| Repo layout | **In the Turborepo**, with a root `docker-compose.yml` for all broker infra. |
| Count storage | **Live in-memory map + periodic Postgres snapshot**; `--replay` rebuilds into a separate map/table without disturbing the live snapshot. |

## Architecture

```
                          ┌─────────────────────────────────────────┐
   HTTP (advance run)     │      backend (NestJS producer)          │
  ──────────────────────► │  RunsModule: run state machine          │
   auth events (hook)     │  EventBus: dual-publish every event     │
                          └───────┬──────────────────────┬──────────┘
                                  │ RMQ transport         │ Kafka transport
                     topic exchange `events`        topic `run-events`
                     routing key = event name       key = workflowId (3 partitions)
                                  │                        │
                    ┌─────────────▼──────────┐   ┌─────────▼───────────────────────┐
                    │ notifier (NestJS RMQ   │   │ audit-go (Go, Kafka consumer     │
                    │ microservice)          │   │ group)                           │
                    │ • manual ack/nack      │   │ • append audit_log               │
                    │ • DLQ on failure       │   │ • live count map + DB snapshot   │
                    │ • writes notification  │   │ • --replay from offset 0         │
                    │   rows → Postgres      │   │ • --lib=kafka-go | sarama        │
                    └────────────────────────┘   └──────────────────────────────────┘
                    ephemeral: ack = gone          replayable: log persists
```

**One event, two destinations.** Every run advance (and every wired auth event) is published
to RabbitMQ **and** Kafka simultaneously. Two independent consumers then process it in
fundamentally different ways.

### New units in the monorepo

- `apps/notifier/` — a second NestJS app, a pure microservice (`NestFactory.createMicroservice`),
  RabbitMQ transport. Its one job: consume events → write `notification` rows.
- `apps/audit-go/` — a Go module (outside the pnpm/turbo build graph), Kafka consumer group.
- `docker-compose.yml` (root) — Postgres, RabbitMQ (+ management UI), Kafka (KRaft, no
  Zookeeper), kafka-ui, and the audit-go service.

Each unit has one clear purpose, communicates only through the brokers + shared Postgres
schema, and can be run and understood independently.

## Data model (Drizzle, added alongside existing auth schema)

| Table | Written by | Columns (essential) |
|---|---|---|
| `workflow` | backend | `id (uuid pk)`, `name text`, `createdAt` |
| `run` | backend | `id (uuid pk)`, `workflowId → workflow.id`, `status ('queued'\|'running'\|'completed'\|'failed')`, `step int`, `createdAt`, `updatedAt` |
| `notification` | notifier | `id (uuid pk)`, `runId (uuid, nullable)`, `type text`, `payload jsonb`, `createdAt` |
| `audit_log` | audit-go | `id (bigserial pk)`, `eventType text`, `workflowId uuid`, `runId uuid`, `payload jsonb`, `partition int`, `offset bigint`, `createdAt` |
| `workflow_run_counts` | audit-go | `workflowId (uuid pk)`, `completedCount int`, `updatedAt` |

Migrations via the existing `drizzle-kit` setup. The notifier imports the Drizzle schema
from the backend package (or a shared package) rather than redefining it.

## Component design

### 1. Producer — backend `RunsModule` + `EventBusService`

- `POST /runs` → creates a `workflow` (if needed) + a `run` in `queued`.
- `POST /runs/:id/advance` → advances the state machine
  (`queued → running → completed`, or `→ failed`). Each transition calls `EventBus.emit`.
- State machine is a pure, unit-testable function: `(currentStatus) → nextStatus`.
- `EventBusService` holds two `ClientProxy` instances via `ClientsModule`:
  - **RMQ client**: `exchange: 'events'`, `exchangeType: 'topic'`, `wildcards: true`, so
    `emit('run.advanced', payload)` publishes with routing key `run.advanced`.
  - **Kafka client**: topic `run-events`, message **key = `workflowId`** (guarantees a
    workflow's events share a partition → per-workflow ordering).
- `emit(event)` fans out to **both** clients.
- **Auth as a second producer**: a better-auth `after`/database hook emits
  `auth.user.created` and `auth.session.created` through the same `EventBus`.

Event names (routing keys / logical types): `run.advanced`, `run.completed`, `run.failed`,
`auth.user.created`, `auth.session.created`.

### 2. RabbitMQ consumer — `apps/notifier` (smart broker)

Uses the Nest RMQ transporter, configured to keep the primitives **visible**:

- **Topic exchange** `events`. Queue `notifications` bound with `run.#` and `auth.#`.
  The routing key is read at runtime via `context.getMessage().fields.routingKey`.
- **Manual ack** (`noAck: false`): the handler writes the `notification` row, then
  `channel.ack(msg)`. On failure it calls `channel.nack(msg, false, false)` (no requeue).
- **DLQ**: the `notifications` queue is declared with
  `arguments: { 'x-dead-letter-exchange': 'events.dlx' }`; `events.dlx` routes to a
  `notifications.dlq` queue. Nacked messages land there, inspectable in the management UI.
- **Ephemeral by design**: once a message is acked it is gone — there is no way to replay it.

### 3. Kafka consumer — `apps/audit-go` (replayable log)

- Topic `run-events`, **3 partitions**, consumer group `audit`. Offsets committed as the
  service progresses.
- Pipeline of goroutines connected by channels:
  `consumer goroutine → aggregator goroutine → snapshotter goroutine`.
  - **consumer**: reads messages, appends each to `audit_log` (recording `partition` + `offset`).
  - **aggregator**: maintains `map[workflowID]int`, incrementing on `run.completed`.
  - **snapshotter**: every N seconds writes the map to `workflow_run_counts`.
- **Two library backends** behind one `Consumer` interface, chosen by `--lib=kafka-go|sarama`,
  to contrast reader ergonomics, offset commit, and rebalance handling. The count aggregator
  is pure and shared by both.
- **`--replay` mode**: joins with a fresh consumer group and reads from **offset 0**,
  rebuilding counts into a *separate* map/table without touching the live snapshot — proving
  state is reconstructable from the log alone.

## Infrastructure (`docker-compose.yml`)

Services: `postgres`, `rabbitmq` (management UI on `:15672`), `kafka` (KRaft single broker,
no Zookeeper), `kafka-ui`, `audit-go` (built from `apps/audit-go/Dockerfile`).
`docker compose up` starts everything; `drizzle-kit push` applies the schema.
`scripts/demo.sh` curls `/runs/:id/advance` in a loop so both pipelines light up.

## Acceptance criteria (how the contrast is *seen*)

1. Advancing runs produces `notification` rows (Rabbit) **and** `audit_log` rows + updated
   `workflow_run_counts` (Kafka).
2. Forcing a handler failure routes the message to `notifications.dlq`; a successfully
   acked message is gone forever (no replay possible).
3. Stopping audit-go, producing more events, then restarting it → it **resumes from the last
   committed offset** (pull, offset-based).
4. Running `audit-go --replay` rebuilds the completed-per-workflow counts from offset 0.
   Attempting the same reconstruction from RabbitMQ is impossible — the queue is drained.
5. An auth event (e.g. user sign-up) flows through the same pipeline as a second producer.

## Testing

- **Jest (backend):** run state-machine transitions; `EventBus` fan-out to both clients
  (brokers mocked).
- **Go unit test:** the count aggregator as a pure function over an event slice.
- **End-to-end:** `scripts/demo.sh` against the running compose stack, verified by inspecting
  Postgres tables and the broker management UIs.

## Implementation sequencing (for the plan)

1. Run/workflow domain + `EventBusService` dual-publish (producer only; log emits).
2. RabbitMQ notifier: exchange/queue/DLQ, manual ack, `notification` rows.
3. Kafka audit-go: consumer group, `audit_log`, live count map + snapshot (start with one lib).
4. Second Go lib behind the interface; `--replay` mode; auth-event producer; demo script.

Each milestone gets its own plan → execution cycle.
