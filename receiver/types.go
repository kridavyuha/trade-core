package receiver

import (
	"context"

	"github.com/kridavyuha/trade-core/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer interface {
	NewConsumer(string, string) (Consumer, error)
	Start(context.Context, *types.DataWrapper) error
	Close()
}

type RabbitMQConsumer struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue amqp.Queue
}
