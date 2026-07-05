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
