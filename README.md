# better-auth

A Turborepo monorepo pairing a **better-auth authentication stack** (NestJS API + Next.js frontend) with a **RabbitMQ-vs-Kafka dual-broker event pipeline** — a hands-on exercise in the two messaging paradigms.

When a workflow run advances (or a user signs up), the backend publishes the **same event to two consumers**:

- a **RabbitMQ** consumer (`apps/notifier`) — *smart broker*: push, per-message ack, routing keys, one requeue then DLQ, **ephemeral** (once acked, gone).
- a **Kafka** consumer (`apps/audit-go`) — *dumb replayable log*: pull, offset-based, partitions, consumer groups, **replayable** (rebuild state from offset 0).

That contrast is the lesson: the Go service can `--replay` the completed-run counts from offset 0; the drained Rabbit queue cannot.

## Layout

```
apps/
  backend/     NestJS API — better-auth + the run/workflow domain + dual-publish EventBus
  web/         Next.js frontend — sign-in / sign-up / password reset (better-auth client)
  notifier/    NestJS RabbitMQ microservice — consumes events, writes notification rows
  audit-go/    Go Kafka consumer — kafka-go | sarama, audit_log + counts + replay
packages/
  db/          @repo/db — shared Drizzle schema, connection factory, migrations
  eslint-config, typescript-config
docker-compose.yml   RabbitMQ + Kafka (KRaft) + an isolated demo Postgres (:5433)
scripts/demo.sh      one-command end-to-end demo of both pipelines
```

## Prerequisites

- Node ≥ 18, **pnpm 9** (`corepack enable`)
- Docker + Docker Compose (for the brokers and demo Postgres)
- A Postgres for local dev (the demo uses its own on port 5433)
- Go is **not** required on the host — `apps/audit-go` builds and runs via the `golang:1.25` container

## Setup

```sh
pnpm install

# backend env — see apps/backend/README.md for the full list
#   DATABASE_URL, BETTER_AUTH_SECRET, BETTER_AUTH_URL, TRUSTED_ORIGINS, SMTP_*, RABBITMQ_URL, KAFKA_BROKERS

# apply the schema to your dev database
DATABASE_URL=postgres://.../yourdb pnpm --filter @repo/db exec drizzle-kit migrate
```

## Run the auth app

```sh
pnpm --filter @repo/db build     # backend/notifier consume @repo/db as a built package
pnpm --filter backend dev        # NestJS API on :3000
pnpm --filter web dev            # Next.js frontend on :3001
```

## Run the dual-broker demo

One command brings up the brokers + an **isolated** demo Postgres (`:5433`, your dev DB is never touched), starts all three services, drives `/runs/:id/advance` plus a sign-up, and prints both pipelines and a Kafka replay:

```sh
./scripts/demo.sh
```

You'll see `notification` rows (RabbitMQ), `audit_log` + `workflow_run_counts` (Kafka), and the counts rebuilt from offset 0.

### Poke at it manually

```sh
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic run-events --partitions 3 --replication-factor 1

# create + advance a run
curl -X POST localhost:3000/runs -H 'content-type: application/json' -d '{"workflow":"demo"}'
curl -X POST localhost:3000/runs/<id>/advance

# rebuild the Kafka counts from offset 0 — the move RabbitMQ can't make
docker run --rm --network better-auth_default -v "$PWD/apps/audit-go":/src -w /src \
  golang:1.25 go run ./cmd/audit --replay --lib sarama --brokers kafka:29092
```

Broker UIs: RabbitMQ management at http://localhost:15673 (guest/guest), Kafka on `localhost:9092`.

## Common commands

```sh
pnpm --filter backend test        # NestJS unit tests
pnpm --filter notifier test
docker run --rm -v "$PWD/apps/audit-go":/src -w /src golang:1.25 go test -race ./...
pnpm --filter @repo/db exec drizzle-kit generate   # new migration after schema edits
```

## The details live in each app

- [`apps/backend`](apps/backend/README.md) — the producer + auth
- [`apps/notifier`](apps/notifier/README.md) — the RabbitMQ consumer
- [`apps/audit-go`](apps/audit-go/README.md) — the Kafka consumer
