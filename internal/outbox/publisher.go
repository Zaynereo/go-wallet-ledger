// Package outbox delivers ledger events that were committed with the
// financial transaction. Publishing is at-least-once; consumers dedupe on
// event_id.
package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Event is one unpublished outbox row.
type Event struct {
	ID          int64
	EventID     string
	EventType   string
	AggregateID string
	Payload     []byte
	CreatedAt   time.Time
}

// Publisher sends a claimed batch. Implementations must be safe for one
// caller at a time; the relay does not overlap batches on a single instance.
type Publisher interface {
	Publish(ctx context.Context, events []Event) error
	Close() error
}

// LogPublisher writes each event as a structured log line. It is the default
// so the service runs without a broker.
type LogPublisher struct {
	logger *slog.Logger
}

func NewLogPublisher(logger *slog.Logger) *LogPublisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogPublisher{logger: logger}
}

func (p *LogPublisher) Publish(ctx context.Context, events []Event) error {
	for _, event := range events {
		p.logger.InfoContext(ctx, "outbox_event",
			"event_id", event.EventID,
			"event_type", event.EventType,
			"aggregate_id", event.AggregateID,
			"payload", json.RawMessage(event.Payload),
		)
	}
	return nil
}

func (p *LogPublisher) Close() error { return nil }

// KafkaPublisher publishes to a single topic with acks=all. Keys are the
// transaction id so events for one aggregate stay on one partition.
type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireAll,
			Async:                  false,
			AllowAutoTopicCreation: true,
			BatchTimeout:           10 * time.Millisecond,
		},
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, events []Event) error {
	msgs := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		msgs = append(msgs, kafka.Message{
			Key:   []byte(event.AggregateID),
			Value: event.Payload,
			Time:  event.CreatedAt,
		})
	}
	return p.writer.WriteMessages(ctx, msgs...)
}

func (p *KafkaPublisher) Close() error { return p.writer.Close() }
