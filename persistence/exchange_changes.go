package persistence

import (
	"fmt"
	"github.com/open-horizon/anax/cutil"
	"time"
)

// Constants used throughout the code.
const EXCHANGE_CHANGES = "exchange-change-state" // The bucket name in the bolt DB.

type ChangeState struct {
	ChangeID    uint64 `json:"changeId"`
	LastUpdated int64  `json:"lastUpdated"`
}

func (c ChangeState) String() string {
	lu := time.Unix(c.LastUpdated, 0).Format(cutil.ExchangeTimeFormat)
	return fmt.Sprintf("Change State ID: %v, last updated: %v", c.ChangeID, lu)
}

// Retrieve the change state object from the database. The bolt APIs assume there is more than 1 object in a bucket,
// so this function has to be prepared for that case, even though there should only ever be 1.
func FindExchangeChangeState(db AgentDatabase) (*ChangeState, error) {
	return db.FindExchangeChangeState()
}

// There is only 1 object in the bucket so we can use the bucket name as the object key.
func SaveExchangeChangeState(db AgentDatabase, changeID uint64) error {
	return db.SaveExchangeChangeState(changeID)
}

// Remove the change state object from the local database.
func DeleteExchangeChangeState(db AgentDatabase) error {
	return db.DeleteExchangeChangeState()
}
