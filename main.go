package main

import (
	"context"
	"fmt"
	"log"

	"github.com/kridavyuha/trade-core/receiver"
)

// write a http server here which listens on port 8082 and
func main() {
	db, err := initDB()
	if err != nil {
		log.Panic("Unable to start the database")
	}

	rabbitMQConsumer := &receiver.RabbitMQConsumer{}
	consumer, err := rabbitMQConsumer.NewConsumer("", "")
	if err != nil {
		log.Panic("Unable to create the consumer")
	}

	ctx, cancel := context.WithCancel(context.TODO())
	go func() {
		err := consumer.Start(ctx)
		if err != nil {
			fmt.Printf("unable to start the consumer")
			cancel()
		}
	}()

	// Think about a way to not let the main goroutine close -- servers or something else
}
