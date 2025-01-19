package receiver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	amqp "github.com/rabbitmq/amqp091-go"

	types "github.com/kridavyuha/trade-core/types"
)

func (c *RabbitMQConsumer) NewConsumer(url, queueName string) (Consumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare(
		"txns",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}

	return &RabbitMQConsumer{
		conn,
		ch,
		queue,
	}, nil
}

func (c *RabbitMQConsumer) Start(ctx context.Context) error {
	msgs, err := c.ch.ConsumeWithContext(
		ctx,
		c.queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}
			if err := c.processMessage(msg); err != nil {
				log.Printf("error processing message: %v", err)
				msg.Nack(false, true)
			} else {
				msg.Ack(false)
			}
		}
	}
}

func (c *RabbitMQConsumer) processMessage(msg amqp.Delivery) error {
	msgBody := msg.Body
	trnx := &types.TrnxMsg{}
	if err := json.Unmarshal(msgBody, trnx); err != nil {
		return fmt.Errorf("unable to Unmarshall transaction msg body into : %T with err: %w", reflect.TypeOf(trnx), err)
	}
	// Extra logic goes here like updating the price in db & redis
	return nil
}

func (c *RabbitMQConsumer) Close() {
	if c.ch != nil {
		c.ch.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
