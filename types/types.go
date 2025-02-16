package types

import (
	"time"

	"github.com/kridavyuha/api-server/pkg/kvstore"
	"gorm.io/gorm"
)

type TrnxMsg struct {
	LeagueID        string `json:"league_id"`
	PlayerID        string `json:"player_id"`
	NumOfShares     int    `json:"shares"`
	UserId          int    `json:"user_id"`
	TransactionType string `json:"transaction_type"`
}

type DataWrapper struct {
	DB    *gorm.DB
	Cache kvstore.KVStore
}

type League struct {
	LeagueID     string    `json:"league_id"`
	LeagueStatus string    `json:"league_status"`
	CreatedAt    time.Time `json:"created_at"`
}
