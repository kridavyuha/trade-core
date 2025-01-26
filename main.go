package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kridavyuha/trade-core/receiver"
	"github.com/kridavyuha/trade-core/types"
)

var leagueStatusMap = make(map[string]LeagueStatus)

func main() {
	dataTier := &types.DataWrapper{}
	err := initDB(dataTier)
	if err != nil {
		log.Panic("Unable to start the database")
	}
	initKVStore(dataTier)

	rabbitMQurl := "amqp://guest:guest@localhost:5672/"
	err = initProducer(rabbitMQurl)
	if err != nil {
		log.Panic("Unable to start the producer")
	}

	rabbitMQConsumer := &receiver.RabbitMQConsumer{}
	consumer, err := rabbitMQConsumer.NewConsumer(rabbitMQurl, "txns")
	if err != nil {
		log.Panic("Unable to create the consumer")
	}

	var forever chan struct{}
	ctx, cancel := context.WithCancel(context.TODO())
	go func() {
		err := consumer.Start(ctx, dataTier)
		if err != nil {
			fmt.Printf("unable to start the consumer: %w", err)
			cancel()
		}
	}()

	fetchLeagueStatusFromDBTicker := time.NewTicker(5 * time.Minute)
	go fetchLeaguesStatusFromDB(ctx, fetchLeagueStatusFromDBTicker, dataTier)

	<-forever
}
