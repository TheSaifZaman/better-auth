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
