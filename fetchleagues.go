package main

import (
	"context"
	"fmt"
	"time"

	"github.com/kridavyuha/trade-core/receiver"
	"github.com/kridavyuha/trade-core/types"
)

func fetchLeaguesStatusFromDB(rabbitmq *receiver.RabbitMQ, ctx context.Context, ticker *time.Ticker, dataTier *types.DataWrapper) {
	for {
		select {
		case <-ticker.C:
			// Fetch the leagues table from the database and get the leagues which are not in status 'active'
			var leagues []types.League
			res := dataTier.DB.Raw("SELECT league_id, league_status, created_at, starts_at FROM leagues WHERE league_status != 'active'").Scan(&leagues)
			if res.Error != nil {
				fmt.Println("Error fetching leagues from the database: %w", res.Error)
				continue
			}
			// Now go through the leagues slice and for each league of status `not started` see if the map already has that status, if yes, skip, if not, call a goroutine to attach the queue to the exchange at starts_at time
			// Similarly, if the league status is 'completed', but not already reflected in the map, then write a goroutine to detach the queue from the exchange. If already reflected, skip.
			for _, league := range leagues {
				if league.LeagueStatus == string(leagueStatusNotStarted) {
					if _, exists := leagueStatusMap[league.LeagueID]; !exists {
						leagueStatusMap[league.LeagueID] = leagueStatusNotStarted
						// Create a timer that will trigger at the `starts_at` time of the league
						go attachQueueToExchangeAtStartsAtTime(rabbitmq, league.StartsAt, league, dataTier)
					}
				}
				if league.LeagueStatus == string(leagueStatusCompleted) {
					if status, exists := leagueStatusMap[league.LeagueID]; !exists || status != leagueStatusCompleted {
						leagueStatusMap[league.LeagueID] = leagueStatusCompleted
						go detachQueueFromExchange(rabbitmq, league, dataTier)
					}
				}
			}
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func attachQueueToExchangeAtStartsAtTime(rabbitmq *receiver.RabbitMQ, startTime time.Time, league types.League, dataTier *types.DataWrapper) {
	timer := time.NewTimer(time.Until(league.StartsAt))
	<-timer.C
	// Create a queue and bind it to the exchange
	queueName := fmt.Sprintf("league_%s", league.LeagueID)
	exchangeName := "txns"
	routingKey := fmt.Sprintf("league.%s", league.LeagueID)
	consumer, err := rabbitmq.NewConsumer(queueName, exchangeName, routingKey)
	if err != nil {
		fmt.Println("Failed to create a consumer for league %s: %w", league.LeagueID, err)
		return
	}
	leagueConsumerMap[league.LeagueID] = consumer
	// Start the consumer
	consumer.StartConsuming(context.Background(), routingKey, dataTier) // needs to be reevaluated--TODO: @anveshreddy18
}

func detachQueueFromExchange(rabbitmq *receiver.RabbitMQ, league types.League, dataTier *types.DataWrapper) {
	if consumer, exists := leagueConsumerMap[league.LeagueID]; exists {
		routingKey := fmt.Sprintf("league.%s", league.LeagueID)
		consumer.CloseConsumer(routingKey)
		delete(leagueConsumerMap, league.LeagueID)
	}
}
