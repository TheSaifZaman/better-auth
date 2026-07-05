package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"audit-go/internal/consumer"
	"audit-go/internal/consumer/factory"
	"audit-go/internal/count"
	"audit-go/internal/event"
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
	lib := flag.String("lib", "kafka-go", "kafka client library: kafka-go | sarama")
	replay := flag.Bool("replay", false, "replay the log from offset 0 and print rebuilt counts")
	replayIdle := flag.Duration("replay-idle", 5*time.Second, "replay stops after this idle gap")
	flag.Parse()

	if *dsn == "" {
		*dsn = os.Getenv("DATABASE_URL")
	}

	if *replay {
		runReplay(*lib, *brokers, *topic, *replayIdle)
		return
	}
	runNormal(*lib, *brokers, *topic, *group, *dsn, *snapshotEvery, *fromBeginning)
}

func runNormal(lib, brokers, topic, group, dsn string, snapshotEvery time.Duration, fromBeginning bool) {
	st, err := store.Open(dsn)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	cfg := consumer.Config{
		Brokers:       []string{brokers},
		Topic:         topic,
		GroupID:       group,
		FromBeginning: fromBeginning,
	}
	c, err := factory.New(lib, cfg)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pipe := pipeline.New(st, snapshotEvery)
	pipe.Start(ctx)

	log.Printf("audit-go consuming lib=%s topic=%s group=%s", lib, topic, group)
	if err := c.Run(ctx, pipe.Handle); err != nil {
		log.Printf("consumer stopped: %v", err)
	}
	pipe.Stop()
	log.Print("audit-go stopped")
}

// runReplay rebuilds the completed-per-workflow counts purely from the Kafka log,
// reading an ephemeral consumer group from offset 0 and printing the result.
// It writes nothing to the database — the whole point is that state is
// reconstructable from the log alone (impossible against a drained RabbitMQ queue).
func runReplay(lib, brokers, topic string, idle time.Duration) {
	group := fmt.Sprintf("audit-replay-%d", time.Now().UnixNano()) // ephemeral group

	cfg := consumer.Config{
		Brokers:       []string{brokers},
		Topic:         topic,
		GroupID:       group,
		FromBeginning: true,
		IdleTimeout:   idle,
	}
	c, err := factory.New(lib, cfg)
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
