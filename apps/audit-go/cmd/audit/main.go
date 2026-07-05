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
