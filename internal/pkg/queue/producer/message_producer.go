package producer

import (
	"app/my_app/internal/pkg/queue"
	"app/my_app/internal/server/config"
	"app/my_app/internal/server/controller/dto"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type MessageProducer struct {
	producer queue.Producer
}

type MessagePublisher interface {
	ReceiveMessage(ctx context.Context, msg dto.MessageRaw) error
}

func NewMessageProduer(config config.Config) MessagePublisher {
	hosts := strings.Split(config.Connection.Kafka.Hosts, ",")
	topic := config.Connection.Kafka.Message.Topic
	producer := queue.NewProducer(hosts, topic, queue.RequiredAcksAll)

	return &MessageProducer{producer: producer}
}

func (p *MessageProducer) ReceiveMessage(ctx context.Context, msg dto.MessageRaw) error {
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("json marshal failed: %w", err)
	}

	err = p.producer.Produce(ctx, queue.Message{
		Key:   []byte(msg.ID),
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("produce message failed: %w", err)
	}
	return nil
}
