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
