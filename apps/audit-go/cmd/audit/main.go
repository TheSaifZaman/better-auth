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
