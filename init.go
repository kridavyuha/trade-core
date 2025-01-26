package main

import (
	"encoding/json"
	"fmt"

	"github.com/kridavyuha/api-server/pkg/kvstore"
	"github.com/kridavyuha/trade-core/types"
	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func initDB(w *types.DataWrapper) error {
	dsn := "host=localhost user=postgres password=postgres dbname=db port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}
	w.DB = db
	return nil
}

func initKVStore(w *types.DataWrapper) {
	w.Cache = kvstore.NewRedis("localhost:6379", "", 0)
}

func initProducer(url string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("Failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Failed to open a channel: %w", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		"txns", // name
		false,  // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	if err != nil {
		return fmt.Errorf("Failed to declare a queue: %w", err)
	}

	transactionData := map[string]interface{}{
		"player_id":        1,
		"league_id":        2,
		"user_id":          3,
		"transaction_type": "BUY",
		"shares":           5,
	}

	transactionJSON, err := json.Marshal(transactionData)
	if err != nil {
		return fmt.Errorf("Failed to marshal transaction data: %w", err)
	}

	// Publish the transaction to the queue
	err = ch.Publish(
		"",     // exchange
		"txns", // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        transactionJSON,
		})
	if err != nil {
		return fmt.Errorf("Failed to publish a message: %w", err)
	}
	return nil
}
