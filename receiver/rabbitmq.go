package receiver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
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
				batchStartTimestamp = time.Now()
			}
			fmt.Println("Msg timestamp: ", msg.Timestamp)
			fmt.Println("Batch start timestamp: ", batchStartTimestamp)
			if time.Since(batchStartTimestamp) <= time.Second {
				transactionMsgList = append(transactionMsgList, trnx)
				// print the transactionMsgList
				fmt.Println("printing the transactionMsgList")
				for _, trnx := range transactionMsgList {
					fmt.Println("TransactionMsgList: ", trnx)
				}
			}
		default:
			if time.Since(batchStartTimestamp) > time.Second {
				if len(transactionMsgList) != 0 {
					//TODO: This batch should be computed as quickly as possible, so that the next batch can be processed.
					// We should have SLA's for this.
					// TODO: why not to have diffent 1 sec batches for different users, so that it may be more fair.
					transactionProcessingWorker(dataTier, transactionMsgList)
					transactionMsgList = []*types.TrnxMsg{}
				}
				batchStartTimestamp = time.Now()
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

func processTransactionList(dataTier *types.DataWrapper, transactionMsgList []*types.TrnxMsg) {
	fmt.Println("Processing the transactions in bulk ", transactionMsgList)

	var wg sync.WaitGroup
	wg.Add(len(transactionMsgList))

	for _, trnx := range transactionMsgList {
		go func(trnx *types.TrnxMsg) {
			defer wg.Done()
			fmt.Println("Processing the transaction: ", trnx)
			Transaction(dataTier, trnx)
		}(trnx)
	}

	wg.Wait()
}

func updatePlayersCurPrice(dataTier *types.DataWrapper, playerID string, transactionMsg *types.PlayerTransaction) error {
	var avgNetShares float64 = float64(transactionMsg.NetChange) / float64(transactionMsg.UserCount)
	avgNetShares = avgNetShares * 1 // This factor needs rethink & should make it configurable.
	if err := updatePlayerPrice(dataTier, playerID, transactionMsg.LeagueID, avgNetShares); err != nil {
		log.Printf("failed to update the player price: %v", err)
		return err
	}
	return nil
}

func processTransactionOfPlayerWhenNetNegative(dataTier *types.DataWrapper, playerID string, transactionMsgList []*types.TrnxMsg, playerTransactionMap map[string]*types.PlayerTransaction) {
	// First step::
	// For each user transaction, give them the shares they asked for, or want to sell at the current price of the player as fetched from the player table
	// use goroutines to do the transactions

	processTransactionList(dataTier, transactionMsgList)

	// Second step::
	// loop through the list and for each player(identified by their PlayerID), identify the avg share count, which is used to compute the price by which the player price needs to be updated.
	// It is computed as follows => avgShareCount*(somefactor). Now there could be some buys and there could be some sells, so get the net number based on the transaction type and divide by the number of users who requested for the transaction
	// of this particular player.

	updatePlayersCurPrice(dataTier, playerID, calulateNetSharesPerPlayer(transactionMsgList)[playerID])
}

func processTransactionOfPlayerWhenNetPositveOrZero(dataTier *types.DataWrapper, playerID string, transactionMsgList []*types.TrnxMsg, playerTransactionMap map[string]*types.PlayerTransaction) {
	// First step::
	// loop through the list and for each player(identified by their PlayerID), identify the avg share count, which is used to compute the price by which the player price needs to be updated.
	// It is computed as follows => avgShareCount*(somefactor). Now there could be some buys and there could be some sells, so get the net number based on the transaction type and divide by the number of users who requested for the transaction
	// of this particular player.

	updatePlayersCurPrice(dataTier, playerID, calulateNetSharesPerPlayer(transactionMsgList)[playerID])

	// Second step::
	// For each user transaction, give them the shares they asked for, or want to sell at the current price of the player as fetched from the player table
	// use goroutines to do the transactions
	processTransactionList(dataTier, transactionMsgList)
}

func calulateNetSharesPerPlayer(transactionMsgList []*types.TrnxMsg) map[string]*types.PlayerTransaction {

	fmt.Printf("TransactionMsgList: %v\n", transactionMsgList)
	var playerTransactionMap = make(map[string]*types.PlayerTransaction)
	for _, trnx := range transactionMsgList {
		if _, ok := playerTransactionMap[trnx.PlayerID]; !ok {
			playerTransactionMap[trnx.PlayerID] = &types.PlayerTransaction{NetChange: 0, UserCount: 0, LeagueID: ""}
		}
		playerTransactionMap[trnx.PlayerID].UserCount++
		if trnx.TransactionType == transactionTypeBuy {
			playerTransactionMap[trnx.PlayerID].NetChange += trnx.NumOfShares
		} else {
			playerTransactionMap[trnx.PlayerID].NetChange -= trnx.NumOfShares
		}
		playerTransactionMap[trnx.PlayerID].LeagueID = trnx.LeagueID
	}
	return playerTransactionMap
}

func transactionProcessingWorker(dataTier *types.DataWrapper, transactionMsgList []*types.TrnxMsg) {
	// PreReq :
	playerTransactionMap := calulateNetSharesPerPlayer(transactionMsgList)

	for playerID, transaction := range playerTransactionMap {
		// get all the transactions of this player from the transactionMsgList
		playersTransactionMsgList := make([]*types.TrnxMsg, 0)
		for _, trnx := range transactionMsgList {
			if trnx.PlayerID == playerID {
				playersTransactionMsgList = append(playersTransactionMsgList, trnx)
			}
		}

		if transaction.NetChange >= 0 {
			// TODO: handle these goroutines properly..
			// Here lets say a user buying 2 players in this same batch, and here while updating his purse these go routines update the same field for the same row...
			// so need to be carefully handled.
			go processTransactionOfPlayerWhenNetPositveOrZero(dataTier, playerID, playersTransactionMsgList, playerTransactionMap)
		} else {
			go processTransactionOfPlayerWhenNetNegative(dataTier, playerID, playersTransactionMsgList, playerTransactionMap)
		}
	}
}

func updatePlayerPrice(dataTier *types.DataWrapper, playerID string, leagueID string, avgNetShares float64) error {
	// Get the player price list from the cache
	playerPriceList, err := getPlayerPriceList(dataTier, leagueID, playerID)
	if err != nil {
		return fmt.Errorf("failed to get the player price list: %v", err)
	}

	// Get the last price and timestamp
	priceAndTimestamp := strings.Split(playerPriceList[len(playerPriceList)-1], ",")
	if len(priceAndTimestamp) != 2 {
		return fmt.Errorf("invalid data format for price and timestamp")
	}

	curPrice, err := strconv.ParseFloat(priceAndTimestamp[1], 64)
	if err != nil {
		return fmt.Errorf("failed to convert the price to int: %v", err)
	}

	// Update the price
	newPrice := curPrice + avgNetShares*(0.05/100)*curPrice

	// add entry in cache
	err = dataTier.Cache.RPush("players_"+leagueID+"_"+playerID, fmt.Sprintf("%d,%.2f", time.Now().Unix(), newPrice))
	if err != nil {
		return fmt.Errorf("failed to update the player price in cache: %v", err)
	}
	// also update into a players_<league_id> map
	err = dataTier.Cache.HSet("players_"+leagueID, playerID, fmt.Sprintf("%.2f", newPrice))
	if err != nil {
		return fmt.Errorf("failed to update the player price in cache (players_<league_id> key): %v", err)
	}
	// update to players_<league_id> table
	err = dataTier.DB.Exec("UPDATE players_"+leagueID+" SET cur_price = ? WHERE player_id = ?", newPrice, playerID).Error
	if err != nil {
		return fmt.Errorf("failed to update the player price in database: %v", err)
	}

	return nil
}

func getPlayerPriceList(dataTier *types.DataWrapper, leagueId, playerId string) ([]string, error) {
	players, err := dataTier.Cache.LRange("players_"+leagueId+"_"+playerId, 0, -1)

	if err != nil {
		return nil, err
	}

	if len(players) == 0 {
		// TODO: load the table data into cache.
		// If the player is not found in the cache, get the player details from the players table
		// Load players from the players table to the cache
		// If the player is not found in the players table, return an error
		return nil, fmt.Errorf("player not found")
	}
	return players, nil
}

func getPurse(dataTier *types.DataWrapper, userId int, leagueId string) (string, error) {
	balanceAndRemainingTxnsStr, err := dataTier.Cache.Get("purse_" + strconv.Itoa(userId) + "_" + leagueId)
	if err != nil {
		if err == redis.Nil {
			// TODO: load table data into cache
			// Load the purse from the table to cache
			// if not in table return err.
		} else {
			return "0", err
		}
	}

	return balanceAndRemainingTxnsStr, nil
}

func Transaction(dataTier *types.DataWrapper, transactionDetails *types.TrnxMsg) error {

	// Here we simultaneously update the transactions, purse and portfolio tables in db and cache for consistency
	// the purchase rate will be calculated by the core service once the transaction is successful this will be sent to queue.
	fmt.Println("Came to Transaction function")
	players, err := getPlayerPriceList(dataTier, transactionDetails.LeagueID, transactionDetails.PlayerID)
	if err != nil {
		return err
	}
	fmt.Println("Got the player price list: ", players)

	priceAndTs := strings.Split(players[len(players)-1], ",")
	if len(priceAndTs) != 2 {
		return fmt.Errorf("invalid data format for price and timestamp")
	}
	fmt.Println("Got the price and timestamp: ", priceAndTs)

	curPrice, err := strconv.ParseFloat(priceAndTs[1], 64)
	if err != nil {
		return err
	}
	fmt.Println("Got the current price: ", curPrice)
	// ----------------------------------------------
	// Get the user's balance from the purse table

	balanceAndRemainingTxnsStr, err := getPurse(dataTier, transactionDetails.UserId, transactionDetails.LeagueID)
	if err != nil {
		return err
	}
	fmt.Println("Got the user's balance and remaining txns: ", balanceAndRemainingTxnsStr)

	// ----------------------------------------------
	var shares int

	// Check in portfolio_user_id_league_id hash for this player_id field
	_, err = dataTier.Cache.HGet("portfolio_"+strconv.Itoa(transactionDetails.UserId)+"_"+transactionDetails.LeagueID, "is_cached")
	if err != nil {
		if err == redis.Nil {
			//TODO: fetch the portfolio from the table and load it into cache, along side is_cached active
		} else {
			return err
		}
	}
	sharesInvestedStr, err := dataTier.Cache.HGet("portfolio_"+strconv.Itoa(transactionDetails.UserId)+"_"+transactionDetails.LeagueID, transactionDetails.PlayerID)
	fmt.Println("Got the shares and invested: ", sharesInvestedStr)

	switch {
	case err == redis.Nil:
		sharesInvestedStr = "-1,0"
	case err != nil:
		return err
	}

	fmt.Println("share and invested: ", sharesInvestedStr)

	sharesInvested := strings.Split(sharesInvestedStr, ",")
	if len(sharesInvested) != 2 {
		return fmt.Errorf("invalid data format for shares and invested")
	}
	fmt.Println("Got the shares and invested: ", sharesInvested)

	shares, err = strconv.Atoi(sharesInvested[0])
	if err != nil {
		return err
	}
	fmt.Println("Got the shares: ", shares)

	balance, err := strconv.ParseFloat(strings.Split(balanceAndRemainingTxnsStr, ",")[0], 64)
	if err != nil {
		return err
	}

	txns, err := strconv.Atoi(strings.Split(balanceAndRemainingTxnsStr, ",")[1])
	if err != nil {
		return err
	}

	// Check if the user has enough balance to buy the shares
	if transactionDetails.TransactionType == "buy" {
		// subtract the amount from the user's balance
		balance -= float64(transactionDetails.NumOfShares) * curPrice
		txns--
	} else if transactionDetails.TransactionType == "sell" {
		balance += float64(transactionDetails.NumOfShares) * curPrice
	}

	var txnID int
	err = dataTier.DB.Raw("INSERT INTO transactions (user_id, player_id, league_id, shares, price, transaction_type, transaction_time) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id", transactionDetails.UserId, transactionDetails.PlayerID, transactionDetails.LeagueID, transactionDetails.NumOfShares, curPrice, transactionDetails.TransactionType, time.Now()).Scan(&txnID).Error
	if err != nil {
		return err
	}

	// Also update the purse table
	err = dataTier.DB.Exec("UPDATE purse SET remaining_purse = ?, remaining_transactions = ? WHERE user_id = ? AND league_id = ?", balance, txns, transactionDetails.UserId, transactionDetails.LeagueID).Error
	if err != nil {
		return err
	}

	balanceRounded := fmt.Sprintf("%.2f", balance)
	remainingTxns := strconv.Itoa(txns)
	// Update the user's balance in cache ..
	err = dataTier.Cache.Set("purse_"+strconv.Itoa(transactionDetails.UserId)+"_"+transactionDetails.LeagueID, balanceRounded+","+remainingTxns)
	if err != nil {
		return err
	}

	// Update the portfolio table
	// If the player is already present in the portfolio, update the shares and average price
	// If the player is not present, insert a new row
	err = UpdatePortfolio(dataTier, transactionDetails, shares, curPrice)
	if err != nil {
		return err
	}

	err = InsertNotifiction(dataTier, txnID, curPrice, transactionDetails)
	if err != nil {
		return err
	}

	return nil
}

func UpdatePortfolio(dataTier *types.DataWrapper, transactionDetails *types.TrnxMsg, shares int, curPrice float64) error {
	fmt.Println("Came to UpdatePortfolio function")
	if transactionDetails.TransactionType == "buy" {
		fmt.Printf("In buy , Existing Shares: %d, current Price: %f", shares, curPrice)
		// Update or insert the shares if player is already in the portfolio
		if shares == -1 {
			// Update in table first...
			err := dataTier.DB.Exec("INSERT INTO portfolio (user_id, player_id, league_id, shares) VALUES (?, ?, ?, ?)", transactionDetails.UserId, transactionDetails.PlayerID, transactionDetails.LeagueID, transactionDetails.NumOfShares).Error
			if err != nil {
				return err
			}

			// Update in cache
			err = dataTier.Cache.HSet("portfolio_"+strconv.Itoa(transactionDetails.UserId)+"_"+transactionDetails.LeagueID, transactionDetails.PlayerID, strconv.Itoa(transactionDetails.NumOfShares)+","+strconv.Itoa(0))
			if err != nil {
				return err
			}
		} else {

			err := dataTier.DB.Exec("UPDATE portfolio SET shares = shares + ? WHERE user_id = ? AND player_id = ? AND league_id = ?", transactionDetails.NumOfShares, transactionDetails.UserId, transactionDetails.PlayerID, transactionDetails.LeagueID).Error
			if err != nil {
				return err
			}
			// Update in cache
			shares += transactionDetails.NumOfShares
			err = dataTier.Cache.HSet("portfolio_"+strconv.Itoa(transactionDetails.UserId)+"_"+transactionDetails.LeagueID, transactionDetails.PlayerID, strconv.Itoa(shares)+","+strconv.Itoa(0))
			if err != nil {
				return err
			}
		}
	} else if transactionDetails.TransactionType == "sell" {
		// Update or delete the shares if player is already in the portfolio
		fmt.Printf("In Sell , Existing Shares: %d, current Price: %f", shares, curPrice)
		shares -= transactionDetails.NumOfShares

		err := dataTier.DB.Exec("UPDATE portfolio SET shares = ? WHERE user_id = ? AND player_id = ? AND league_id = ?", shares, transactionDetails.UserId, transactionDetails.PlayerID, transactionDetails.LeagueID).Error
		if err != nil {
			return err
		}
		// update in cache ..
		err = dataTier.Cache.HSet("portfolio_"+strconv.Itoa(transactionDetails.UserId)+"_"+transactionDetails.LeagueID, transactionDetails.PlayerID, strconv.Itoa(shares)+","+strconv.Itoa(0))
		if err != nil {
			return err
		}
	}

	return nil
}

func InsertNotifiction(dataTier *types.DataWrapper, txnID int, txnPrice float64, transactionDetails *types.TrnxMsg) error {

	var entity_type int
	if transactionDetails.TransactionType == "sell" {
		entity_type = 2
	} else {
		entity_type = 1
	}

	// insert into notif_obj
	var notif_obj_id int
	err := dataTier.DB.Raw("INSERT into notification_obj (entity_type_id, entity_id, created_at) VALUES (?, ?, ?) RETURNING id", entity_type, txnID, time.Now()).Scan(&notif_obj_id).Error

	if err != nil {
		return fmt.Errorf("unable to insert notif_obj, err: %v", err)
	}

	// insert into notificaiton
	err = dataTier.DB.Exec("INSERT into notification (notification_obj_id,notifier_id,status,actor_id) VALUES (?,?,?,?)", notif_obj_id, transactionDetails.UserId, "unseen", 1).Error
	if err != nil {
		return fmt.Errorf("unable to insert into notification, err: %v", err)
	}

	return nil
}
