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
