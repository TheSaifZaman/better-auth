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
