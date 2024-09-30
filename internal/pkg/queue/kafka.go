package queue

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
	"time"
)

type Message = kafka.Message

type Consumer interface {
	Consume(context.Context) (Message, error)
	FetchMessage(context.Context) (Message, error)
	CommitMessage(context.Context, ...Message) error
	Start() kafka.ReaderStats
	Close() error
}

type consumer struct {
	reader *kafka.Reader
	topic  string
	group  string
}

func (c *consumer) Consume(ctx context.Context) (Message, error) {
	m, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("kafka consume failed: [topic=%s, group=%s]: %w",
			c.topic, c.group, err)
	}
	return m, nil
}

func (c *consumer) FetchMessage(ctx context.Context) (Message, error) {
	m, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("kafka fetch failed: [topic=%s, group=%s]: %w",
			c.topic, c.group, err)
	}
	return m, nil
}

func (c *consumer) CommitMessage(ctx context.Context, messages ...Message) error {
	err := c.reader.CommitMessages(ctx)
	if err != nil {
		return fmt.Errorf("kafka commit failed: [topic=%s, group=%s]: %w",
			c.topic, c.group, err)
	}
	return nil
}

func (c *consumer) Start() kafka.ReaderStats {
	return c.reader.Stats()
}

func (c *consumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}

func NewConsumer(hosts []string, topic, group string) Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         hosts,
		Topic:           topic,
		GroupID:         group,
		MaxBytes:        10e6,
		MaxWait:         10 * time.Millisecond,
		ReadLagInterval: -1,
		ReadBackoffMax:  15 * time.Millisecond,
		ReadBackoffMin:  10 * time.Millisecond,
	})

	return &consumer{
		reader: reader,
		topic:  topic,
		group:  group,
	}
}

type RequiredAcks = kafka.RequiredAcks

const (
	RequiredAcksNone = kafka.RequireNone
	RequiredAcksOne  = kafka.RequireOne
	RequiredAcksAll  = kafka.RequireAll
)

type Producer interface {
	Produce(context.Context, ...Message) error
	Start() kafka.WriterStats
	Close() error
}

type producer struct {
	write        *kafka.Writer
	requiredAcks RequiredAcks
	topic        string
}

func (p *producer) Produce(ctx context.Context, messages ...Message) error {
	if err := p.write.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("kafka produce failed: [topic=%s, ack=%s]: %w",
			p.topic, p.requiredAcks, err)
	}
	return nil
}

func (p *producer) Start() kafka.WriterStats {
	return p.write.Stats()
}

func (p *producer) Close() error {
	if p.write != nil {
		return p.write.Close()
	}
	return nil
}

func NewProducer(hosts []string, topic string, requiredAcks RequiredAcks) Producer {
	if isValidRequiredAck(requiredAcks) {
		panic("kafka.NewProducer: unknown required acks")
	}
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          hosts,
		Topic:            topic,
		Balancer:         new(kafka.Hash),
		RequiredAcks:     int(requiredAcks),
		Async:            true,
		BatchTimeout:     time.Millisecond * 10,
		CompressionCodec: snappy.NewCompressionCodec(),
	})
	return &producer{
		write:        writer,
		requiredAcks: requiredAcks,
		topic:        topic,
	}
}

func isValidRequiredAck(mode RequiredAcks) bool {
	switch mode {
	case RequiredAcksNone, RequiredAcksOne, RequiredAcksAll:
		return true
	default:
		return false
	}
}
