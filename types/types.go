package types

type TrnxMsg struct {
	LeagueName  string `json:"league_name"`
	PlayerName  string `json:"player_name"`
	NumOfShares int    `json:"shares_count"`
}
