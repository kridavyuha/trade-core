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

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

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

func (c *RabbitMQConsumer) Start(ctx context.Context, dataTier *types.DataWrapper) error {
	fmt.Println("Came to start: queue name: ", c.queue.Name)
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
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			c.Close()
			return nil
		case msg, ok := <-msgs:
			if !ok {
				c.Close()
				return nil
			}
			if err := c.processMessage(msg, dataTier); err != nil {
				log.Printf("error processing message: %v", err)
				msg.Nack(false, true)
			} else {
				msg.Ack(false)
			}
		}
	}
}

func (c *RabbitMQConsumer) processMessage(msg amqp.Delivery, dataTier *types.DataWrapper) error {
	msgBody := msg.Body
	fmt.Println(msg.Timestamp)
	trnx := &types.TrnxMsg{}
	if err := json.Unmarshal(msgBody, trnx); err != nil {
		return fmt.Errorf("unable to Unmarshall transaction msg body into : %T with err: %w", reflect.TypeOf(trnx), err)
	}
	// Extra logic goes here like updating the price in db & redis
	fmt.Println("Received the transaction object: ", trnx)
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
