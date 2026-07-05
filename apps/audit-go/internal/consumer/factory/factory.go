package factory

import (
	"fmt"

	"audit-go/internal/consumer"
	"audit-go/internal/consumer/kafkago"
	saramaconsumer "audit-go/internal/consumer/sarama"
)

// New builds a Consumer for the named library ("kafka-go" or "sarama").
func New(lib string, cfg consumer.Config) (consumer.Consumer, error) {
	switch lib {
	case "kafka-go":
		return kafkago.New(cfg), nil
	case "sarama":
		return saramaconsumer.New(cfg)
	default:
		return nil, fmt.Errorf("unknown --lib %q (want kafka-go or sarama)", lib)
	}
}
