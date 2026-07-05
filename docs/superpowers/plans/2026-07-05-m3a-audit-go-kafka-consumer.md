# Milestone 3a — Go Audit Service (kafka-go) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `apps/audit-go`, a Go service that consumes the `run-events` Kafka topic with a consumer group (segmentio/kafka-go), appends every event to `audit_log`, and maintains a live per-workflow completed-count that a background goroutine snapshots to `workflow_run_counts`.

**Architecture:** A single consumer goroutine owns the kafka-go reader: `FetchMessage` → append to `audit_log` (recording partition + offset) → signal completions over a channel → `CommitMessages` (explicit offset commit). An aggregator goroutine ranges the completions channel and increments a mutex-guarded counter; a snapshotter goroutine persists the counter on a ticker. Kafka runs in KRaft mode (no Zookeeper) in the existing `docker-compose.yml`; the topic has 3 partitions keyed by `workflowId`. Go is built and run via `golang:1.23` containers (no host Go install).

**Tech Stack:** Go 1.23, `github.com/segmentio/kafka-go`, `github.com/lib/pq`, `database/sql`, Apache Kafka 3.8 (KRaft), Docker.

## Global Constraints

- No host Go toolchain. Build/test/run through `golang:1.23` containers. The canonical test command is:
  `docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 go test ./...`
- The Go module path is `audit-go`; entrypoint at `cmd/audit`, publisher helper at `cmd/publish`.
- `audit_log.workflow_id` / `run_id` are **text** (not uuid) and have **no FK** — this is a decoupled append log of whatever crosses the broker, and demo events use non-uuid ids like `w1`.
- Kafka columns are named `kafka_partition` / `kafka_offset` (avoid the reserved word `offset`).
- Kafka listeners: containers use `kafka:29092`; host uses `localhost:9092`.
- Topic `run-events`, 3 partitions, replication factor 1; key = `workflowId` (hash → partition).
- Offsets are committed explicitly per message (`FetchMessage` + `CommitMessages`), never auto-committed.
- The demo Postgres (compose, port 5433, db `betterauth`) is the only DB migrated against; the developer's 5432 is untouched.

---

### Task 1: Add audit tables to `@repo/db`

**Files:**
- Create: `packages/db/src/schema/audit.ts`
- Modify: `packages/db/src/schema/index.ts`

**Interfaces:**
- Produces (re-exported from `@repo/db`): `auditLog`, `workflowRunCounts` tables.

- [ ] **Step 1: Create the audit schema**

Create `packages/db/src/schema/audit.ts`:
```ts
import {
  pgTable,
  bigserial,
  bigint,
  integer,
  text,
  jsonb,
  timestamp,
} from 'drizzle-orm/pg-core';

// Append-only log written by the Go audit service. Decoupled from the
// producer tables: workflow_id / run_id are plain text with no FK, because a
// consumer across a broker boundary should not share referential integrity
// with the producer.
export const auditLog = pgTable('audit_log', {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  eventType: text('event_type').notNull(),
  workflowId: text('workflow_id'),
  runId: text('run_id'),
  payload: jsonb('payload').notNull(),
  kafkaPartition: integer('kafka_partition').notNull(),
  kafkaOffset: bigint('kafka_offset', { mode: 'number' }).notNull(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

// Live per-workflow completed-run count, snapshotted by the audit service.
export const workflowRunCounts = pgTable('workflow_run_counts', {
  workflowId: text('workflow_id').primaryKey(),
  completedCount: integer('completed_count').notNull().default(0),
  updatedAt: timestamp('updated_at')
    .defaultNow()
    .$onUpdate(() => /* @__PURE__ */ new Date())
    .notNull(),
});
```

- [ ] **Step 2: Export it from the barrel**

In `packages/db/src/schema/index.ts`, add:
```ts
export * from './audit';
```

- [ ] **Step 3: Build and generate the migration**

Run:
```bash
cd /Users/nocturnal/Gitlab/better-auth
pnpm --filter @repo/db build
pnpm --filter @repo/db exec drizzle-kit generate
```
Expected: a `packages/db/drizzle/0003_*.sql` creating `audit_log` and `workflow_run_counts`; drizzle-kit reports 10 tables with the two audit tables new.

- [ ] **Step 4: Verify the migration content**

Run: `cat packages/db/drizzle/0003_*.sql`
Expected: `CREATE TABLE "audit_log"` (with `kafka_partition`, `kafka_offset`, `bigserial` id) and `CREATE TABLE "workflow_run_counts"` (text pk).

- [ ] **Step 5: Commit**

```bash
git add packages/db/src/schema/audit.ts packages/db/src/schema/index.ts packages/db/drizzle
git commit -m "feat(db): add audit_log and workflow_run_counts tables"
```

---

### Task 2: Scaffold the Go module, Dockerfile, and Kafka infra

**Files:**
- Create: `apps/audit-go/go.mod`
- Create: `apps/audit-go/cmd/audit/main.go` (temporary hello, finalized in Task 5)
- Create: `apps/audit-go/Dockerfile`
- Create: `apps/audit-go/.dockerignore`
- Modify: `docker-compose.yml` (add `kafka`)

**Interfaces:**
- Produces: a compiling Go module and a `kafka` compose service.

- [ ] **Step 1: Module manifest**

Create `apps/audit-go/go.mod`:
```
module audit-go

go 1.23

require (
	github.com/lib/pq v1.10.9
	github.com/segmentio/kafka-go v0.4.47
)
```

- [ ] **Step 2: Temporary entrypoint**

Create `apps/audit-go/cmd/audit/main.go`:
```go
package main

import "fmt"

func main() {
	fmt.Println("audit-go")
}
```

- [ ] **Step 3: Dockerfile and dockerignore**

Create `apps/audit-go/Dockerfile`:
```dockerfile
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/audit ./cmd/audit

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/audit /audit
ENTRYPOINT ["/audit"]
```

Create `apps/audit-go/.dockerignore`:
```
Dockerfile
.dockerignore
```

- [ ] **Step 4: Add Kafka to compose**

In `docker-compose.yml`, add under `services:` (alongside `rabbitmq` and `postgres-demo`):
```yaml
  kafka:
    image: apache/kafka:3.8.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 15
```

- [ ] **Step 5: Tidy, build, validate**

Run:
```bash
cd /Users/nocturnal/Gitlab/better-auth
docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 sh -c "go mod tidy && go build ./..."
docker compose config >/dev/null && echo "compose OK"
```
Expected: `go mod tidy` writes `go.sum`; `go build ./...` succeeds; compose validates.

- [ ] **Step 6: Commit**

```bash
git add apps/audit-go/go.mod apps/audit-go/go.sum apps/audit-go/cmd apps/audit-go/Dockerfile apps/audit-go/.dockerignore docker-compose.yml
git commit -m "chore(audit-go): scaffold Go module, Dockerfile, and Kafka compose service"
```

---

### Task 3: Event parsing and counting (pure, go test)

**Files:**
- Create: `apps/audit-go/internal/event/event.go`
- Create: `apps/audit-go/internal/event/event_test.go`
- Create: `apps/audit-go/internal/count/count.go`
- Create: `apps/audit-go/internal/count/count_test.go`

**Interfaces:**
- Produces:
  - `event.Event` struct + `event.Parse([]byte) (Event, error)` + `(Event).IsCompletion() bool`
  - `count.Counter` (concurrent) with `New()`, `Add(workflowID)`, `Snapshot() map[string]int`

- [ ] **Step 1: Write the failing tests**

Create `apps/audit-go/internal/event/event_test.go`:
```go
package event

import "testing"

func TestParse(t *testing.T) {
	e, err := Parse([]byte(`{"event":"run.completed","workflowId":"w1","runId":"r1","step":2}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.WorkflowID != "w1" || e.Event != "run.completed" || e.Step != 2 {
		t.Fatalf("unexpected event: %+v", e)
	}
	if !e.IsCompletion() {
		t.Fatal("expected completion")
	}
}

func TestParseInvalid(t *testing.T) {
	if _, err := Parse([]byte("not json")); err == nil {
		t.Fatal("expected error for invalid json")
	}
}

func TestIsCompletionFalse(t *testing.T) {
	if (Event{Event: "run.advanced"}).IsCompletion() {
		t.Fatal("run.advanced is not a completion")
	}
}
```

Create `apps/audit-go/internal/count/count_test.go`:
```go
package count

import (
	"sync"
	"testing"
)

func TestCounterAdd(t *testing.T) {
	c := New()
	c.Add("w1")
	c.Add("w1")
	c.Add("w2")
	got := c.Snapshot()
	if got["w1"] != 2 || got["w2"] != 1 {
		t.Fatalf("unexpected counts: %+v", got)
	}
}

func TestSnapshotIsCopy(t *testing.T) {
	c := New()
	c.Add("w1")
	snap := c.Snapshot()
	snap["w1"] = 99
	if c.Snapshot()["w1"] != 1 {
		t.Fatal("snapshot must not alias internal state")
	}
}

func TestConcurrentAdd(t *testing.T) {
	c := New()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Add("w1") }()
	}
	wg.Wait()
	if c.Snapshot()["w1"] != 100 {
		t.Fatalf("expected 100, got %d", c.Snapshot()["w1"])
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 go test ./...
```
Expected: build failures — `event.go` / `count.go` do not exist yet.

- [ ] **Step 3: Write the implementations**

Create `apps/audit-go/internal/event/event.go`:
```go
package event

import "encoding/json"

// Event is the JSON message value the producer publishes to Kafka.
type Event struct {
	Event      string `json:"event"`
	RunID      string `json:"runId"`
	WorkflowID string `json:"workflowId"`
	Status     string `json:"status"`
	Step       int    `json:"step"`
	At         string `json:"at"`
}

// Parse decodes a Kafka message value into an Event.
func Parse(value []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(value, &e); err != nil {
		return Event{}, err
	}
	return e, nil
}

// IsCompletion reports whether the event marks a run completing.
func (e Event) IsCompletion() bool {
	return e.Event == "run.completed"
}
```

Create `apps/audit-go/internal/count/count.go`:
```go
package count

import "sync"

// Counter tracks completed runs per workflow. Safe for concurrent use.
type Counter struct {
	mu     sync.RWMutex
	counts map[string]int
}

func New() *Counter {
	return &Counter{counts: make(map[string]int)}
}

func (c *Counter) Add(workflowID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[workflowID]++
}

// Snapshot returns a copy of the current counts.
func (c *Counter) Snapshot() map[string]int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]int, len(c.counts))
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}
```

- [ ] **Step 4: Run tests (with the race detector)**

Run:
```bash
docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 go test -race ./...
```
Expected: all packages PASS, no race warnings.

- [ ] **Step 5: Commit**

```bash
git add apps/audit-go/internal/event apps/audit-go/internal/count
git commit -m "feat(audit-go): add event parsing and concurrent counter"
```

---

### Task 4: Postgres store

**Files:**
- Create: `apps/audit-go/internal/store/store.go`

**Interfaces:**
- Produces:
  - `store.AuditRecord{ EventType, WorkflowID, RunID string; Payload []byte; Partition int; Offset int64 }`
  - `store.Open(dsn string) (*Store, error)`, `(*Store).Close() error`
  - `(*Store).AppendAudit(ctx, AuditRecord) error`
  - `(*Store).UpsertCount(ctx, workflowID string, count int) error`

- [ ] **Step 1: Write the store**

Create `apps/audit-go/internal/store/store.go`:
```go
package store

import (
	"context"
	"database/sql"

	_ "github.com/lib/pq"
)

type AuditRecord struct {
	EventType  string
	WorkflowID string
	RunID      string
	Payload    []byte
	Partition  int
	Offset     int64
}

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// nullable maps an empty id to SQL NULL so text columns stay clean.
func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func (s *Store) AppendAudit(ctx context.Context, r AuditRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO audit_log (event_type, workflow_id, run_id, payload, kafka_partition, kafka_offset)
		 VALUES ($1, $2, $3, $4::jsonb, $5, $6)`,
		r.EventType, nullable(r.WorkflowID), nullable(r.RunID), string(r.Payload), r.Partition, r.Offset)
	return err
}

func (s *Store) UpsertCount(ctx context.Context, workflowID string, count int) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO workflow_run_counts (workflow_id, completed_count, updated_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (workflow_id) DO UPDATE
		   SET completed_count = EXCLUDED.completed_count, updated_at = now()`,
		workflowID, count)
	return err
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 go build ./...
```
Expected: PASS. (Behavior is exercised in the Task 6 integration run.)

- [ ] **Step 3: Commit**

```bash
git add apps/audit-go/internal/store
git commit -m "feat(audit-go): add Postgres store for audit_log and counts"
```

---

### Task 5: Consumer pipeline and entrypoint

**Files:**
- Modify: `apps/audit-go/cmd/audit/main.go`

**Interfaces:**
- Consumes: `event`, `count`, `store`, `kafka-go`.
- Produces: the finalized `audit` binary — consumer goroutine + aggregator + snapshotter, flag-configured.

- [ ] **Step 1: Write the entrypoint**

Replace `apps/audit-go/cmd/audit/main.go` with:
```go
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"audit-go/internal/count"
	"audit-go/internal/event"
	"audit-go/internal/store"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "kafka brokers")
	topic := flag.String("topic", "run-events", "kafka topic")
	group := flag.String("group", "audit", "consumer group")
	dsn := flag.String("database-url", "", "postgres dsn (falls back to DATABASE_URL)")
	snapshotEvery := flag.Duration("snapshot-every", 5*time.Second, "count snapshot interval")
	flag.Parse()

	if *dsn == "" {
		*dsn = os.Getenv("DATABASE_URL")
	}

	st, err := store.Open(*dsn)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{*brokers},
		Topic:   *topic,
		GroupID: *group,
	})
	defer reader.Close()

	counter := count.New()
	completions := make(chan string, 128)
	var wg sync.WaitGroup

	// Aggregator: increments the counter as completions arrive over the channel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for wf := range completions {
			counter.Add(wf)
		}
	}()

	// Snapshotter: persists the live count map on a ticker, plus a final flush.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(*snapshotEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				snapshot(context.Background(), st, counter)
				return
			case <-ticker.C:
				snapshot(ctx, st, counter)
			}
		}
	}()

	log.Printf("audit-go consuming topic=%s group=%s brokers=%s", *topic, *group, *brokers)

	// Consumer loop: fetch -> append audit -> signal completion -> commit offset.
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			break // context cancelled or reader closed
		}
		e, perr := event.Parse(msg.Value)
		if perr != nil {
			log.Printf("skip unparseable message at offset %d: %v", msg.Offset, perr)
			reader.CommitMessages(context.Background(), msg)
			continue
		}
		rec := store.AuditRecord{
			EventType:  e.Event,
			WorkflowID: e.WorkflowID,
			RunID:      e.RunID,
			Payload:    msg.Value,
			Partition:  msg.Partition,
			Offset:     msg.Offset,
		}
		if err := st.AppendAudit(ctx, rec); err != nil {
			log.Printf("append audit (offset %d): %v", msg.Offset, err)
			continue // do not commit; the message will be redelivered
		}
		if e.IsCompletion() {
			completions <- e.WorkflowID
		}
		if err := reader.CommitMessages(context.Background(), msg); err != nil {
			log.Printf("commit (offset %d): %v", msg.Offset, err)
		}
		log.Printf("audited %s partition=%d offset=%d", e.Event, msg.Partition, msg.Offset)
	}

	close(completions)
	wg.Wait()
	log.Print("audit-go stopped")
}

func snapshot(ctx context.Context, st *store.Store, counter *count.Counter) {
	for wf, c := range counter.Snapshot() {
		if err := st.UpsertCount(ctx, wf, c); err != nil {
			log.Printf("upsert count %s: %v", wf, err)
		}
	}
}
```

- [ ] **Step 2: Build**

Run:
```bash
docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 sh -c "go vet ./... && go build ./..."
```
Expected: `go vet` and `go build` both clean.

- [ ] **Step 3: Commit**

```bash
git add apps/audit-go/cmd/audit/main.go
git commit -m "feat(audit-go): consumer pipeline with explicit offset commits and count snapshots"
```

---

### Task 6: End-to-end verification against real Kafka

Adds a Go publisher helper, then runs the full pipeline against the compose stack.

**Files:**
- Create: `apps/audit-go/cmd/publish/main.go`

- [ ] **Step 1: Write the publisher helper**

Create `apps/audit-go/cmd/publish/main.go`:
```go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Publishes a JSON event to run-events, keyed by workflowId (hash -> partition).
func main() {
	brokers := flag.String("brokers", "localhost:9092", "kafka brokers")
	topic := flag.String("topic", "run-events", "kafka topic")
	eventName := flag.String("event", "run.completed", "event name")
	workflow := flag.String("workflow", "w1", "workflow id")
	run := flag.String("run", "r1", "run id")
	flag.Parse()

	w := &kafka.Writer{
		Addr:     kafka.TCP(*brokers),
		Topic:    *topic,
		Balancer: &kafka.Hash{},
	}
	defer w.Close()

	value, _ := json.Marshal(map[string]any{
		"event":      *eventName,
		"workflowId": *workflow,
		"runId":      *run,
		"status":     "completed",
		"step":       2,
		"at":         time.Now().Format(time.RFC3339),
	})
	if err := w.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(*workflow),
		Value: value,
	}); err != nil {
		log.Fatalf("write: %v", err)
	}
	log.Printf("published %s for workflow %s", *eventName, *workflow)
}
```

- [ ] **Step 2: Bring up infra and create the topic**

Run:
```bash
cd /Users/nocturnal/Gitlab/better-auth
docker compose up -d
until [ "$(docker inspect -f '{{.State.Health.Status}}' $(docker compose ps -q kafka))" = healthy ]; do sleep 3; done
until [ "$(docker inspect -f '{{.State.Health.Status}}' $(docker compose ps -q postgres-demo))" = healthy ]; do sleep 2; done
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic run-events --partitions 3 --replication-factor 1
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic run-events
DATABASE_URL=postgres://postgres:postgres@localhost:5433/betterauth pnpm --filter @repo/db exec drizzle-kit migrate
```
Expected: kafka + postgres-demo healthy; `run-events` created with 3 partitions; migrations `0000`–`0003` applied (audit tables present).

- [ ] **Step 3: Publish events across workflows, then run the audit service**

Run (publisher and audit both on the compose network, using the internal listener `kafka:29092`):
```bash
# Compose default network for this project (dir name "better-auth"):
NET=better-auth_default
# publish 2 completions for w1, 1 for w2, and 1 non-completion
for args in "--workflow w1 --run r1" "--workflow w1 --run r2" "--workflow w2 --run r3" "--event run.advanced --workflow w2 --run r4"; do
  docker run --rm --network "$NET" -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 \
    go run ./cmd/publish --brokers kafka:29092 $args
done
# run the audit service for ~8s, then stop it
timeout 8 docker run --rm --network "$NET" -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 \
  go run ./cmd/audit --brokers kafka:29092 --snapshot-every 2s \
  --database-url postgres://postgres:postgres@postgres-demo:5432/betterauth || true
```
Expected: audit logs show `audited run.completed partition=… offset=…` lines across partitions.

- [ ] **Step 4: Verify audit_log and counts**

Run:
```bash
echo "--- audit_log (per partition) ---"
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c \
  "select event_type, workflow_id, kafka_partition, kafka_offset from audit_log order by kafka_partition, kafka_offset;"
echo "--- workflow_run_counts ---"
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c \
  "select workflow_id, completed_count from workflow_run_counts order by workflow_id;"
```
Expected: `audit_log` holds all 4 events with partition/offset recorded; `workflow_run_counts` shows `w1 = 2`, `w2 = 1` (the `run.advanced` did not increment).

- [ ] **Step 5: Tear down**

Run: `docker compose down`

- [ ] **Step 6: Commit**

```bash
git add apps/audit-go/cmd/publish/main.go
git commit -m "feat(audit-go): add Kafka publisher helper for end-to-end verification"
```

---

## Definition of done (Milestone 3a)

- `audit_log` + `workflow_run_counts` exist in `@repo/db` (migration `0003`).
- Go unit tests pass under `-race` (event parsing, concurrent counter).
- Against real Kafka: every event lands in `audit_log` with its partition + offset; `workflow_run_counts` reflects completed-per-workflow (`w1=2`, `w2=1`); offsets are committed explicitly.
- Kafka runs in compose (KRaft, 3-partition topic keyed by workflowId).

## The lesson so far

Kafka **pulled** messages (the consumer fetches), tracked position with **offsets** it committed explicitly, and spread a workflow's events across **partitions** by key. The log persists — unlike RabbitMQ, nothing was consumed away. M3b exploits exactly that with `--replay` from offset 0, and adds a second client library (sarama) behind one interface.

## Next (Plan 3b, separate)

Introduce a `Consumer` interface, add an IBM/sarama implementation selectable via `--lib=kafka-go|sarama`, and add `--replay` (fresh group from offset 0) that rebuilds the counts from the log — the move the drained RabbitMQ queue cannot make.
