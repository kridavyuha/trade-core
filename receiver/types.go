package receiver

import (
	"context"

	"github.com/kridavyuha/trade-core/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer interface {
	StartConsuming(context.Context, string, *types.DataWrapper) error
	CloseConsumer(string) error
}

type RabbitMQConsumer struct {
	channel *amqp.Channel
	queue   amqp.Queue
}

type RabbitMQ struct {
	conn *amqp.Connection
}

const (
	transactionTypeBuy  = "buy"
	transactionTypeSell = "sell"
)
