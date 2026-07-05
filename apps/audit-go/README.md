# audit-go

Go **Kafka consumer** for the [better-auth monorepo](../../README.md) — the *replayable-log* half of the dual-broker exercise. It consumes the `run-events` topic, appends every event to `audit_log`, and maintains a per-workflow completed-run count snapshotted to `workflow_run_counts`.

## What it demonstrates (Kafka = dumb replayable log)

- **Pull + explicit offsets** — the consumer `FetchMessage`s and commits offsets itself (never auto-commit).
- **Partitions by key** — events are keyed by `workflowId`, so one workflow's events share a partition (ordered).
- **Consumer groups** — normal mode joins the `audit` group and resumes from committed offsets.
- **Replay** — `--replay` joins a *fresh, ephemeral* group from **offset 0** and rebuilds the counts purely from the log, writing nothing to the DB. This is impossible against RabbitMQ's drained queue — the whole point.
- **Two client libraries, one interface** — `--lib kafka-go|sarama` selects the implementation behind `consumer.Consumer`, to contrast their ergonomics.

Concurrency: a consumer goroutine feeds completions over a channel to an aggregator goroutine (mutex-guarded count map); a snapshotter goroutine persists on a ticker. Graceful `SIGTERM` shutdown flushes a final snapshot.

## No host Go required

Everything runs via the `golang:1.25` container (Go ≥1.25 is required by IBM/sarama):

```sh
GO='docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.25'

# test (race detector)
docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.25 go test -race ./...

# run against the compose stack (internal listener kafka:29092, demo Postgres)
docker run --rm --network better-auth_default -v "$PWD/apps/audit-go":/src -w /src \
  -v audit-go-cache:/go/pkg/mod golang:1.25 \
  go run ./cmd/audit --lib kafka-go --group audit --from-beginning --brokers kafka:29092 \
  --database-url "postgres://postgres:postgres@postgres-demo:5432/betterauth?sslmode=disable"
```

> The Postgres DSN must include `?sslmode=disable` for the non-SSL demo database.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--lib` | `kafka-go` | `kafka-go` or `sarama` |
| `--brokers` | `localhost:9092` | comma-separated brokers |
| `--topic` | `run-events` | topic to consume |
| `--group` | `audit` | consumer group |
| `--database-url` | `$DATABASE_URL` | Postgres DSN (`?sslmode=disable`) |
| `--from-beginning` | `false` | start at oldest offset |
| `--snapshot-every` | `5s` | count-snapshot interval |
| `--replay` | `false` | rebuild counts from offset 0 and print (no DB writes) |
| `--replay-idle` | `5s` | replay stops after this idle gap |

## Replay (the payoff)

```sh
docker run --rm --network better-auth_default -v "$PWD/apps/audit-go":/src -w /src \
  golang:1.25 go run ./cmd/audit --replay --lib sarama --brokers kafka:29092
# → rebuilt completed-run counts (from Kafka offset 0):  <workflow> = N
```

## Layout

```
cmd/audit      consumer entrypoint (normal + --replay)
cmd/publish    helper to publish a test event to run-events
internal/
  event/       message parsing + IsCompletion
  count/       concurrent count map
  store/       Postgres audit_log + workflow_run_counts
  consumer/    Consumer interface, kafka-go & sarama impls, factory
  pipeline/    audit + count + snapshot pipeline (library-agnostic)
```

Tables live in [`@repo/db`](../../packages/db) (`audit_log`, `workflow_run_counts`); apply with `drizzle-kit migrate`.
