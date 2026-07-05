# Milestone 3b — Second Kafka Library + Replay — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put both `segmentio/kafka-go` and `IBM/sarama` behind one `Consumer` interface selectable with `--lib`, and add `--replay` that rebuilds the completed-per-workflow counts from Kafka offset 0 — the reconstruction the drained RabbitMQ queue cannot perform.

**Architecture:** A `consumer.Consumer` interface (`Run(ctx, Handler)` / `Close()`) abstracts the two libraries; a `consumer.Config` carries brokers/topic/group plus `FromBeginning` and `IdleTimeout`. The message-processing side moves into a `pipeline.Pipeline` (audit append + completion channel + aggregator + snapshotter) that is library-agnostic and unit-tested against a fake store. Normal mode drives the pipeline against Postgres; replay mode spins up an ephemeral consumer group from offset 0, stops after an idle gap, and prints the rebuilt counts without touching the DB.

**Tech Stack:** Go 1.23, `segmentio/kafka-go`, `IBM/sarama`, `database/sql`/`lib/pq`, Docker.

## Global Constraints

- Build/test/run through `golang:1.23` containers (canonical test command as in 3a):
  `docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 go test -race ./...`
- The `Consumer` interface is the only Kafka surface `main` and `pipeline` see; neither imports a Kafka library directly.
- Offsets are still committed explicitly (kafka-go `CommitMessages`; sarama `MarkMessage`).
- Replay uses a **fresh, unique consumer group** and `FromBeginning`, so it never disturbs the live `audit` group's committed offsets, and writes **no** DB rows.
- Go DSN must include `?sslmode=disable` for the non-SSL demo Postgres.
- No host `timeout`: run long-lived consumers as detached containers and `docker stop`; replay self-terminates on idle.
- Demo Postgres on 5433 only; developer's 5432 untouched.

---

### Task 1: Consumer interface, kafka-go behind it, and the Pipeline

**Files:**
- Create: `apps/audit-go/internal/consumer/consumer.go`
- Create: `apps/audit-go/internal/consumer/kafkago/kafkago.go`
- Create: `apps/audit-go/internal/pipeline/pipeline.go`
- Create: `apps/audit-go/internal/pipeline/pipeline_test.go`

**Interfaces:**
- Produces:
  - `consumer.Message{ Key []byte; Value []byte; Partition int; Offset int64 }`
  - `consumer.Handler = func(ctx, Message) error`
  - `consumer.Config{ Brokers []string; Topic, GroupID string; FromBeginning bool; IdleTimeout time.Duration }`
  - `consumer.Consumer` interface (`Run`, `Close`)
  - `kafkago.New(consumer.Config) *kafkago.Consumer`
  - `pipeline.Store` interface, `pipeline.New(store, snapshotEvery) *Pipeline`, `Start`, `Handle`, `Stop`

- [ ] **Step 1: Define the interface**

Create `apps/audit-go/internal/consumer/consumer.go`:
```go
package consumer

import (
	"context"
	"time"
)

// Message is a library-agnostic Kafka record.
type Message struct {
	Key       []byte
	Value     []byte
	Partition int
	Offset    int64
}

// Handler processes one message. Returning nil permits the offset to be committed.
type Handler func(ctx context.Context, msg Message) error

// Config configures a consumer regardless of the underlying library.
type Config struct {
	Brokers       []string
	Topic         string
	GroupID       string
	FromBeginning bool          // start at oldest when the group has no committed offset
	IdleTimeout   time.Duration // if >0, Run returns after this idle gap (replay)
}

// Consumer abstracts kafka-go and sarama behind one surface.
type Consumer interface {
	// Run consumes messages, invoking handler for each and committing offsets
	// after the handler returns nil. Blocks until ctx is cancelled, or (when
	// IdleTimeout > 0) until no message arrives within the idle window.
	Run(ctx context.Context, handler Handler) error
	Close() error
}
```

- [ ] **Step 2: kafka-go implementation**

Create `apps/audit-go/internal/consumer/kafkago/kafkago.go`:
```go
package kafkago

import (
	"context"
	"errors"
	"time"

	"audit-go/internal/consumer"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	idle   time.Duration // when >0, Run returns after this idle gap (replay)
}

func New(cfg consumer.Config) *Consumer {
	rc := kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
	}
	if cfg.FromBeginning {
		rc.StartOffset = kafka.FirstOffset
	}
	return &Consumer{reader: kafka.NewReader(rc), idle: cfg.IdleTimeout}
}

func (c *Consumer) Run(ctx context.Context, handler consumer.Handler) error {
	for {
		fetchCtx := ctx
		var cancel context.CancelFunc
		if c.idle > 0 {
			fetchCtx, cancel = context.WithTimeout(ctx, c.idle)
		}
		msg, err := c.reader.FetchMessage(fetchCtx)
		if cancel != nil {
			cancel()
		}
		if err != nil {
			// Idle window elapsed with the parent still alive => replay done.
			if c.idle > 0 && errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				return nil
			}
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		m := consumer.Message{Key: msg.Key, Value: msg.Value, Partition: msg.Partition, Offset: msg.Offset}
		if herr := handler(ctx, m); herr != nil {
			continue // do not commit; message will be redelivered
		}
		if cerr := c.reader.CommitMessages(context.Background(), msg); cerr != nil {
			return cerr
		}
	}
}

func (c *Consumer) Close() error { return c.reader.Close() }
```

- [ ] **Step 3: Write the failing pipeline test**

Create `apps/audit-go/internal/pipeline/pipeline_test.go`:
```go
package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"audit-go/internal/consumer"
	"audit-go/internal/store"
)

type fakeStore struct {
	mu     sync.Mutex
	audits []store.AuditRecord
	counts map[string]int
}

func newFakeStore() *fakeStore { return &fakeStore{counts: map[string]int{}} }

func (f *fakeStore) AppendAudit(_ context.Context, r store.AuditRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.audits = append(f.audits, r)
	return nil
}

func (f *fakeStore) UpsertCount(_ context.Context, wf string, c int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts[wf] = c
	return nil
}

func msg(value string) consumer.Message {
	return consumer.Message{Value: []byte(value), Partition: 0, Offset: 0}
}

func TestPipelineAuditsAndCounts(t *testing.T) {
	fs := newFakeStore()
	p := New(fs, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)

	if err := p.Handle(ctx, msg(`{"event":"run.completed","workflowId":"w1"}`)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := p.Handle(ctx, msg(`{"event":"run.completed","workflowId":"w1"}`)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := p.Handle(ctx, msg(`{"event":"run.advanced","workflowId":"w1"}`)); err != nil {
		t.Fatalf("handle: %v", err)
	}

	cancel()
	p.Stop()

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.audits) != 3 {
		t.Fatalf("expected 3 audit rows, got %d", len(fs.audits))
	}
	if fs.counts["w1"] != 2 {
		t.Fatalf("expected w1=2 completions, got %d", fs.counts["w1"])
	}
}

func TestPipelineSkipsUnparseable(t *testing.T) {
	fs := newFakeStore()
	p := New(fs, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	if err := p.Handle(ctx, msg("not json")); err != nil {
		t.Fatalf("unparseable should be skipped, got %v", err)
	}
	cancel()
	p.Stop()
	if len(fs.audits) != 0 {
		t.Fatalf("expected no audit rows for unparseable message")
	}
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run:
```bash
docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 go test ./internal/pipeline/...
```
Expected: build failure — `pipeline` package does not exist yet.

- [ ] **Step 5: Write the Pipeline**

Create `apps/audit-go/internal/pipeline/pipeline.go`:
```go
package pipeline

import (
	"context"
	"log"
	"sync"
	"time"

	"audit-go/internal/consumer"
	"audit-go/internal/count"
	"audit-go/internal/event"
	"audit-go/internal/store"
)

// Store is the subset of the Postgres store the pipeline needs.
type Store interface {
	AppendAudit(ctx context.Context, r store.AuditRecord) error
	UpsertCount(ctx context.Context, workflowID string, count int) error
}

type Pipeline struct {
	store         Store
	counter       *count.Counter
	completions   chan string
	snapshotEvery time.Duration
	aggWG         sync.WaitGroup
	snapWG        sync.WaitGroup
}

func New(st Store, snapshotEvery time.Duration) *Pipeline {
	return &Pipeline{
		store:         st,
		counter:       count.New(),
		completions:   make(chan string, 128),
		snapshotEvery: snapshotEvery,
	}
}

// Start launches the aggregator (channel -> counter) and snapshotter (ticker -> DB).
func (p *Pipeline) Start(ctx context.Context) {
	p.aggWG.Add(1)
	go func() {
		defer p.aggWG.Done()
		for wf := range p.completions {
			p.counter.Add(wf)
		}
	}()

	p.snapWG.Add(1)
	go func() {
		defer p.snapWG.Done()
		ticker := time.NewTicker(p.snapshotEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.flush(ctx)
			}
		}
	}()
}

// Handle is the consumer.Handler: append to the audit log and, for completions,
// signal the aggregator. Returning an error prevents the offset commit.
func (p *Pipeline) Handle(ctx context.Context, msg consumer.Message) error {
	e, err := event.Parse(msg.Value)
	if err != nil {
		log.Printf("skip unparseable offset %d: %v", msg.Offset, err)
		return nil // poison message: commit past it
	}
	rec := store.AuditRecord{
		EventType:  e.Event,
		WorkflowID: e.WorkflowID,
		RunID:      e.RunID,
		Payload:    msg.Value,
		Partition:  msg.Partition,
		Offset:     msg.Offset,
	}
	if err := p.store.AppendAudit(ctx, rec); err != nil {
		return err // do not commit; retry
	}
	if e.IsCompletion() {
		p.completions <- e.WorkflowID
	}
	log.Printf("audited %s partition=%d offset=%d", e.Event, msg.Partition, msg.Offset)
	return nil
}

// Stop drains completions, writes a final snapshot, and waits for goroutines.
func (p *Pipeline) Stop() {
	close(p.completions)
	p.aggWG.Wait()
	p.flush(context.Background()) // fresh ctx: the run ctx is already cancelled
	p.snapWG.Wait()
}

func (p *Pipeline) flush(ctx context.Context) {
	for wf, c := range p.counter.Snapshot() {
		if err := p.store.UpsertCount(ctx, wf, c); err != nil {
			log.Printf("upsert count %s: %v", wf, err)
		}
	}
}
```

- [ ] **Step 6: Run the pipeline tests (with race detector)**

Run:
```bash
docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 go test -race ./internal/pipeline/...
```
Expected: PASS, no races.

- [ ] **Step 7: Rewire main to the interface (kafka-go only for now)**

Replace `apps/audit-go/cmd/audit/main.go` with:
```go
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"audit-go/internal/consumer"
	"audit-go/internal/consumer/kafkago"
	"audit-go/internal/pipeline"
	"audit-go/internal/store"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "kafka brokers")
	topic := flag.String("topic", "run-events", "kafka topic")
	group := flag.String("group", "audit", "consumer group")
	dsn := flag.String("database-url", "", "postgres dsn (falls back to DATABASE_URL)")
	snapshotEvery := flag.Duration("snapshot-every", 5*time.Second, "count snapshot interval")
	fromBeginning := flag.Bool("from-beginning", false, "consume from oldest offset")
	flag.Parse()

	if *dsn == "" {
		*dsn = os.Getenv("DATABASE_URL")
	}

	st, err := store.Open(*dsn)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	cfg := consumer.Config{
		Brokers:       []string{*brokers},
		Topic:         *topic,
		GroupID:       *group,
		FromBeginning: *fromBeginning,
	}
	c := kafkago.New(cfg)
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pipe := pipeline.New(st, *snapshotEvery)
	pipe.Start(ctx)

	log.Printf("audit-go consuming lib=kafka-go topic=%s group=%s", *topic, *group)
	if err := c.Run(ctx, pipe.Handle); err != nil {
		log.Printf("consumer stopped: %v", err)
	}
	pipe.Stop()
	log.Print("audit-go stopped")
}
```

- [ ] **Step 8: Vet and build the whole module**

Run:
```bash
docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 sh -c "go mod tidy && go vet ./... && go build ./... && go test -race ./..."
```
Expected: tidy/vet/build clean; unit tests pass.

- [ ] **Step 9: Commit**

```bash
git add apps/audit-go/internal/consumer apps/audit-go/internal/pipeline apps/audit-go/cmd/audit/main.go apps/audit-go/go.mod apps/audit-go/go.sum
git commit -m "refactor(audit-go): Consumer interface, kafka-go impl, and Pipeline"
```

---

### Task 2: sarama implementation + library factory

**Files:**
- Create: `apps/audit-go/internal/consumer/sarama/sarama.go`
- Create: `apps/audit-go/internal/consumer/factory.go`
- Modify: `apps/audit-go/cmd/audit/main.go` (use factory + `--lib`)

**Interfaces:**
- Produces: `sarama.New(consumer.Config) (*sarama.Consumer, error)`; `consumer.NewByLib(lib string, cfg Config) (Consumer, error)`.

- [ ] **Step 1: sarama consumer**

Create `apps/audit-go/internal/consumer/sarama/sarama.go`:
```go
package sarama

import (
	"context"
	"time"

	"audit-go/internal/consumer"

	saramalib "github.com/IBM/sarama"
)

type Consumer struct {
	group saramalib.ConsumerGroup
	topic string
	idle  time.Duration
}

func New(cfg consumer.Config) (*Consumer, error) {
	c := saramalib.NewConfig()
	c.Version = saramalib.V3_6_0_0
	if cfg.FromBeginning {
		c.Consumer.Offsets.Initial = saramalib.OffsetOldest
	}
	c.Consumer.Offsets.AutoCommit.Enable = true
	c.Consumer.Offsets.AutoCommit.Interval = time.Second

	g, err := saramalib.NewConsumerGroup(cfg.Brokers, cfg.GroupID, c)
	if err != nil {
		return nil, err
	}
	return &Consumer{group: g, topic: cfg.Topic, idle: cfg.IdleTimeout}, nil
}

func (c *Consumer) Run(ctx context.Context, handler consumer.Handler) error {
	if c.idle > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		h := &groupHandler{handler: handler, idle: c.idle, cancel: cancel, reset: make(chan struct{}, 1)}
		go h.watchIdle(ctx)
		return c.consume(ctx, h)
	}
	return c.consume(ctx, &groupHandler{handler: handler})
}

func (c *Consumer) consume(ctx context.Context, h *groupHandler) error {
	for {
		if err := c.group.Consume(ctx, []string{c.topic}, h); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (c *Consumer) Close() error { return c.group.Close() }

type groupHandler struct {
	handler consumer.Handler
	idle    time.Duration
	cancel  context.CancelFunc
	reset   chan struct{}
}

func (h *groupHandler) Setup(saramalib.ConsumerGroupSession) error   { return nil }
func (h *groupHandler) Cleanup(saramalib.ConsumerGroupSession) error { return nil }

func (h *groupHandler) ConsumeClaim(sess saramalib.ConsumerGroupSession, claim saramalib.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		m := consumer.Message{Key: msg.Key, Value: msg.Value, Partition: int(msg.Partition), Offset: msg.Offset}
		if err := h.handler(sess.Context(), m); err != nil {
			continue // do not mark; will be redelivered
		}
		sess.MarkMessage(msg, "") // explicit offset commit (flushed by autocommit)
		if h.reset != nil {
			select {
			case h.reset <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

// watchIdle cancels the run when no message has been marked within idle.
func (h *groupHandler) watchIdle(ctx context.Context) {
	timer := time.NewTimer(h.idle)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.reset:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(h.idle)
		case <-timer.C:
			h.cancel()
			return
		}
	}
}
```

- [ ] **Step 2: Library factory**

Create `apps/audit-go/internal/consumer/factory.go`:
```go
package consumer

import (
	"fmt"

	"audit-go/internal/consumer/kafkago"
	saramaconsumer "audit-go/internal/consumer/sarama"
)

// NewByLib builds a Consumer for the named library ("kafka-go" or "sarama").
func NewByLib(lib string, cfg Config) (Consumer, error) {
	switch lib {
	case "kafka-go":
		return kafkago.New(cfg), nil
	case "sarama":
		return saramaconsumer.New(cfg)
	default:
		return nil, fmt.Errorf("unknown --lib %q (want kafka-go or sarama)", lib)
	}
}
```

- [ ] **Step 3: Use the factory + `--lib` in main**

In `apps/audit-go/cmd/audit/main.go`, remove the `kafkago` import and add a `--lib` flag. Change the flag block to add:
```go
	lib := flag.String("lib", "kafka-go", "kafka client library: kafka-go | sarama")
```
Replace the consumer construction:
```go
	c := kafkago.New(cfg)
	defer c.Close()
```
with:
```go
	c, err := consumer.NewByLib(*lib, cfg)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer c.Close()
```
and update the log line to `lib=%s` using `*lib`. Remove the now-unused `"audit-go/internal/consumer/kafkago"` import.

- [ ] **Step 4: Tidy, vet, build, test**

Run:
```bash
docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 sh -c "go mod tidy && go vet ./... && go build ./... && go test -race ./..."
```
Expected: sarama is downloaded; everything builds; unit tests pass. (If `saramalib.V3_6_0_0` is unknown in the resolved version, use the nearest available `V3_*` / `V2_8_0_0` constant.)

- [ ] **Step 5: Commit**

```bash
git add apps/audit-go/internal/consumer apps/audit-go/cmd/audit/main.go apps/audit-go/go.mod apps/audit-go/go.sum
git commit -m "feat(audit-go): add sarama consumer selectable via --lib"
```

---

### Task 3: Replay mode

**Files:**
- Modify: `apps/audit-go/cmd/audit/main.go`

**Interfaces:**
- Produces: `--replay` + `--replay-idle` flags; replay rebuilds counts from offset 0 and prints them.

- [ ] **Step 1: Add replay to main**

In `apps/audit-go/cmd/audit/main.go`, add the flags:
```go
	replay := flag.Bool("replay", false, "replay the log from offset 0 and print rebuilt counts")
	replayIdle := flag.Duration("replay-idle", 5*time.Second, "replay stops after this idle gap")
```
After `flag.Parse()` and the DSN fallback, branch before opening the store:
```go
	if *replay {
		runReplay(*lib, *brokers, *topic, *replayIdle)
		return
	}
```
Move the existing normal-mode body into a `runNormal(...)` function, or leave it inline after the branch. Then add:
```go
func runReplay(lib, brokers, topic string, idle time.Duration) {
	group := fmt.Sprintf("audit-replay-%d", time.Now().UnixNano()) // ephemeral group

	cfg := consumer.Config{
		Brokers:       []string{brokers},
		Topic:         topic,
		GroupID:       group,
		FromBeginning: true,
		IdleTimeout:   idle,
	}
	c, err := consumer.NewByLib(lib, cfg)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	counter := count.New()
	log.Printf("replay: rebuilding counts from offset 0 (lib=%s group=%s)", lib, group)
	err = c.Run(ctx, func(_ context.Context, msg consumer.Message) error {
		e, perr := event.Parse(msg.Value)
		if perr != nil {
			return nil
		}
		if e.IsCompletion() {
			counter.Add(e.WorkflowID)
		}
		return nil
	})
	if err != nil {
		log.Printf("replay error: %v", err)
	}

	counts := counter.Snapshot()
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("rebuilt completed-run counts (from Kafka offset 0):")
	for _, k := range keys {
		fmt.Printf("  %s = %d\n", k, counts[k])
	}
}
```
Add imports as needed: `"fmt"`, `"sort"`, `"audit-go/internal/count"`, `"audit-go/internal/event"`.

- [ ] **Step 2: Tidy, vet, build, test**

Run:
```bash
docker run --rm -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 sh -c "go mod tidy && go vet ./... && go build ./... && go test -race ./..."
```
Expected: all clean; unit tests still pass.

- [ ] **Step 3: Commit**

```bash
git add apps/audit-go/cmd/audit/main.go apps/audit-go/go.mod apps/audit-go/go.sum
git commit -m "feat(audit-go): add --replay to rebuild counts from offset 0"
```

---

### Task 4: End-to-end — both libraries, then replay

Runbook proving both libraries consume+persist identically, and that replay rebuilds counts purely from the log.

- [ ] **Step 1: Bring up infra, fresh topic, migrate**

Run:
```bash
cd /Users/nocturnal/Gitlab/better-auth
docker compose up -d
until [ "$(docker inspect -f '{{.State.Health.Status}}' $(docker compose ps -q kafka))" = healthy ]; do sleep 3; done
until [ "$(docker inspect -f '{{.State.Health.Status}}' $(docker compose ps -q postgres-demo))" = healthy ]; do sleep 2; done
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic run-events --partitions 3 --replication-factor 1 || true
sleep 5   # let partition metadata propagate (avoids kafka-go Hash balancer race)
DATABASE_URL=postgres://postgres:postgres@localhost:5433/betterauth pnpm --filter @repo/db exec drizzle-kit migrate
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "truncate audit_log; truncate workflow_run_counts;"
```

- [ ] **Step 2: Publish 4 events (persist in the log)**

Run (individually — single publishes are reliable once metadata has settled):
```bash
NET=better-auth_default
GO="docker run --rm --network $NET -v $PWD/apps/audit-go:/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23"
$GO go run ./cmd/publish --brokers kafka:29092 --workflow w1 --run r1
$GO go run ./cmd/publish --brokers kafka:29092 --workflow w1 --run r2
$GO go run ./cmd/publish --brokers kafka:29092 --workflow w2 --run r3
$GO go run ./cmd/publish --brokers kafka:29092 --event run.advanced --workflow w2 --run r4
```
Expected: 4 `published …` lines.

- [ ] **Step 3: Normal consume with kafka-go, verify DB**

Run:
```bash
NET=better-auth_default
docker rm -f auditrun 2>/dev/null
docker run -d --name auditrun --network $NET -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 \
  go run ./cmd/audit --lib kafka-go --from-beginning --group audit-kg --brokers kafka:29092 --snapshot-every 2s \
  --database-url "postgres://postgres:postgres@postgres-demo:5432/betterauth?sslmode=disable"
sleep 18
docker logs auditrun 2>&1 | grep -E "consuming|audited" | tail -8
docker stop auditrun >/dev/null && docker rm auditrun >/dev/null
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "select workflow_id, completed_count from workflow_run_counts order by workflow_id;"
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "select count(*) as audit_rows from audit_log;"
```
Expected: `audited …` lines for 4 events; `workflow_run_counts` = `w1=2, w2=1`; `audit_rows = 4`.

- [ ] **Step 4: Normal consume with sarama, verify DB (fresh tables)**

Run:
```bash
NET=better-auth_default
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "truncate audit_log; truncate workflow_run_counts;"
docker rm -f auditrun 2>/dev/null
docker run -d --name auditrun --network $NET -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 \
  go run ./cmd/audit --lib sarama --from-beginning --group audit-sarama --brokers kafka:29092 --snapshot-every 2s \
  --database-url "postgres://postgres:postgres@postgres-demo:5432/betterauth?sslmode=disable"
sleep 20
docker logs auditrun 2>&1 | grep -E "consuming|audited" | tail -8
docker stop auditrun >/dev/null && docker rm auditrun >/dev/null
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "select workflow_id, completed_count from workflow_run_counts order by workflow_id;"
docker compose exec -T postgres-demo psql -U postgres -d betterauth -c "select count(*) as audit_rows from audit_log;"
```
Expected: same result via sarama — `w1=2, w2=1`, `audit_rows = 4`.

- [ ] **Step 5: Replay from offset 0 with BOTH libraries (no DB)**

Run:
```bash
NET=better-auth_default
echo "=== replay: kafka-go ==="
docker run --rm --network $NET -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 \
  go run ./cmd/audit --replay --lib kafka-go --brokers kafka:29092 --replay-idle 5s 2>&1 | grep -E "rebuilt|=" | tail -6
echo "=== replay: sarama ==="
docker run --rm --network $NET -v "$PWD/apps/audit-go":/src -w /src -v audit-go-cache:/go/pkg/mod golang:1.23 \
  go run ./cmd/audit --replay --lib sarama --brokers kafka:29092 --replay-idle 5s 2>&1 | grep -E "rebuilt|=" | tail -6
```
Expected: both print `w1 = 2` and `w2 = 1`, rebuilt purely from the log at offset 0 — the reconstruction RabbitMQ's drained queue cannot do.

- [ ] **Step 6: Tear down**

Run: `docker compose down`

- [ ] **Step 7: Commit (docs only, if any run notes were added)**

No code changes in this task beyond `cmd/publish` (already committed). Nothing to commit unless notes were captured.

---

## Definition of done (Milestone 3b)

- One `Consumer` interface; `main`/`pipeline` import no Kafka library directly.
- `--lib kafka-go` and `--lib sarama` both consume + persist identically (`w1=2, w2=1`, 4 audit rows).
- `--replay --lib {kafka-go,sarama}` both rebuild `w1=2, w2=1` from offset 0 without writing the DB.
- Pipeline logic is unit-tested (fake store) under `-race`.

## The contrast, complete

RabbitMQ (M2): a **smart broker** that **pushed** messages, took a **per-message ack**, retried once, dead-lettered, and — once acked — **forgot** them. Kafka (M3): a **dumb, replayable log** the consumer **pulled** from at its own **offset**, spread across **partitions** by key; the log **persists**, so `--replay` reconstructs the entire completed-count from offset 0. That reconstruction is impossible against Rabbit's drained queue — which was the whole point.

## Next (Plan 4, separate)

Wire the real backend producer to Kafka + RabbitMQ end-to-end (reconciling the Nest Kafka serialization), add the better-auth `@Hook` auth-event producer, and a `scripts/demo.sh` that drives `/runs/:id/advance` so both pipelines light up from one command.
