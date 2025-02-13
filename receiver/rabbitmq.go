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

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	return &RabbitMQ{conn}, nil
}

func (r *RabbitMQ) CreateExchange(exchangeName string) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel for creating exchange: %w", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare an exchange: %w", err)
	}
	return nil
}

func (rabbitmq *RabbitMQ) NewConsumer(queueName, exchangeName, routingKey string) (Consumer, error) {
	// First create a channel to communicate with the RabbitMQ server
	ch, err := rabbitmq.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}
	// Declare a queue using the channel
	queue, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}
	// Bind this existing queue to a specific exchange with a routing key
	err = ch.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind a queue: %w", err)
	}
	return &RabbitMQConsumer{ch, queue}, nil
}

func (c *RabbitMQConsumer) StartConsuming(ctx context.Context, routingKey string, dataTier *types.DataWrapper) error {
	fmt.Println("Came to start: queue name: ", c.queue.Name)
	msgs, err := c.channel.ConsumeWithContext(
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
			c.CloseConsumer(routingKey)
			return nil
		case msg, ok := <-msgs:
			if !ok {
				c.CloseConsumer(routingKey)
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

func (c *RabbitMQConsumer) CloseConsumer(key string) error {
	err := c.channel.QueueUnbind(
		c.queue.Name,
		key,
		"txns",
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to unbind a queue: %w", err)
	}
	err = c.channel.Close()
	if err != nil {
		return fmt.Errorf("failed to close a channel: %w", err)
	}
	return nil
}
