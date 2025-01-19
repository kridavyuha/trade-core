package receiver

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer interface {
	NewConsumer(string, string) (Consumer, error)
	Start(context.Context) error
	Close()
}

type RabbitMQConsumer struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue amqp.Queue
}
