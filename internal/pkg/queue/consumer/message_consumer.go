package consumer

import (
	"app/my_app/internal/pkg/queue"
	"app/my_app/internal/server/controller/dto"
	"context"
	"encoding/json"
)

type MessageConsumer struct {
	consumer queue.Consumer
}

func NewMessageConsumer(consumer queue.Consumer) *MessageConsumer {
	return &MessageConsumer{consumer: consumer}
}

func (c *MessageConsumer) Run(ctx context.Context, messageHandler func(raw dto.MessageRaw) error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			event, err := c.consumer.FetchMessage(ctx)
			if err != nil {
				continue
			}
			var msg dto.MessageRaw
			err = json.Unmarshal(event.Value, &msg)
			if err != nil {
				continue
			}

			err = messageHandler(msg)
			if err != nil {
				continue
			}
			if err := c.consumer.CommitMessage(ctx); err != nil {
				continue
			}
		}
	}
}
