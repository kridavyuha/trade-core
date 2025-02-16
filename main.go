package main

import (
	"context"
	"log"
	"time"

	"github.com/kridavyuha/trade-core/receiver"
	"github.com/kridavyuha/trade-core/types"
)

var leagueStatusMap = make(map[string]LeagueStatus)
var leagueConsumerMap = make(map[string]receiver.Consumer)

func main() {
	dataTier := &types.DataWrapper{}
	err := initDB(dataTier)
	if err != nil {
		log.Panic("Unable to start the database")
	}
	if err = initKVStore(dataTier); err != nil {
		log.Panic("Unable to start the KV store")
	}

	rabbitMQurl := "amqp://guest:guest@localhost:5672/"
	// err = initProducer(rabbitMQurl)
	// if err != nil {
	// 	log.Panic("Unable to start the producer")
	// }

	rabbitMQ, err := receiver.NewRabbitMQ(rabbitMQurl)
	if err != nil {
		log.Panic("Unable to create the rabbitmq connection")
	}

	// Create a new exchange
	err = rabbitMQ.InitializeExchange("txns")
	if err != nil {
		log.Panic("Unable to create the exchange")
	}

	var forever chan struct{}
	ctx := context.Background()
	// Consumer should be started by the goroutine spinned by the fetchLeaguesStatusFromDB function.
	// Handle the case of stopping the consumer also. Stopping includes closing the channel, unbinding the queue from the exchange and closing the channel finally.

	fetchLeagueStatusFromDBTicker := time.NewTicker(5 * time.Minute)
	go fetchLeaguesStatusFromDB(rabbitMQ, ctx, fetchLeagueStatusFromDBTicker, dataTier)

	<-forever
}
