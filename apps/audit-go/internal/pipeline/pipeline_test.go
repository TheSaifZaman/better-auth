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
