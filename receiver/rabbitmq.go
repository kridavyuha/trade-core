package receiver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

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

func (r *RabbitMQ) InitializeExchange(exchangeName string) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel for creating exchange: %w", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchangeName,
		"direct",
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

	// transactionMsgList is the list of transactions registered by the api-server in a given time frame of 1 second.
	transactionMsgList := make([]*types.TrnxMsg, 0)
	var batchStartTimestamp time.Time

	for {
		select {
		case <-ctx.Done():
			c.CloseConsumer(routingKey)
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}
			msgBody := msg.Body
			trnx := &types.TrnxMsg{}
			if err := json.Unmarshal(msgBody, trnx); err != nil {
				log.Printf("unable to Unmarshall transaction msg body into : %T with err: %v", reflect.TypeOf(trnx), err)
				continue
			}
			fmt.Println("Received the transaction object: ", trnx)
			if len(transactionMsgList) == 0 {
				batchStartTimestamp = msg.Timestamp
			}
			if msg.Timestamp.Sub(batchStartTimestamp) < time.Second {
				transactionMsgList = append(transactionMsgList, trnx)
			} else {
				// Call the transactionProcessingWorker goroutine with the transactionMsgList
				// Reset the transactionMsgList and batchStartTimestamp
				// This is done to batch the transactions and process them in bulk.
				transactionProcessingWorker(dataTier, transactionMsgList)
				transactionMsgList = []*types.TrnxMsg{trnx}
				batchStartTimestamp = msg.Timestamp
			}
		}
	}
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

func transactionProcessingWorker(dataTier *types.DataWrapper, transactionMsgList []*types.TrnxMsg) {
	// First step::
	// For each user transaction, give them the shares they asked for, or want to sell at the current price of the player as fetched from the player table
	// use goroutines to do the transactions

	// Second step::
	// loop through the list and for each player(identified by their PlayerID), identify the avg share count, which is used to compute the price by which the player price needs to be updated.
	// It is computed as follows => avgShareCount*(somefactor). Now there could be some buys and there could be some sells, so get the net number based on the transaction type and divide by the number of users who requested for the transaction
	// of this particular player.
	type playerTransaction struct {
		netChange int
		userCount int
	}
	var playerTransactionMap map[string]*playerTransaction
	for _, trnx := range transactionMsgList {
		playerTransactionMap[trnx.PlayerID].userCount++
		if trnx.TransactionType == transactionTypeBuy {
			playerTransactionMap[trnx.PlayerID].netChange += trnx.NumOfShares
		} else {
			playerTransactionMap[trnx.PlayerID].netChange -= trnx.NumOfShares
		}
	}
	// Loop through the playerTransactionMap and update the prices of the players as per the calculated averages
	for playerID, transaction := range playerTransactionMap {
		var avgNetShares float64 = float64(transaction.netChange) / float64(transaction.userCount)
		priceDiff := avgNetShares * 1 // This factor needs rethink & should make it configurable.
		if err := updatePlayerPrice(playerID, priceDiff); err != nil {
			fmt.Errorf("failed to update the player price: %w", err)
		}
	}
}

func updatePlayerPrice(playerID string, priceDiff float64) error {
	// do the DB work here or call a db utility function from a different package
	return nil
}
