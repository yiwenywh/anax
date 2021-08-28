package persistence

import (
	"fmt"
)

// Constants used throughout the code.
const NODE_EXCH_PATTERN = "nodeexchpattern" // The bucket name in the bolt DB.

// Retrieve the node exchange pattern name from the database. It is set when the exchange node pattern is different
// from the local registered node pattern. It will be cleared once the device pattern get changed.
// The bolt APIs assume there is more than 1 object in a bucket,
// so this function has to be prepared for that case, even though there should only ever be 1.
func FindSavedNodeExchPattern(db AgentDatabase) (string, error) {
	return db.FindSavedNodeExchPattern()
}

// There is only 1 object in the bucket so we can use the bucket name as the object key.
func SaveNodeExchPattern(db AgentDatabase, nodePatternName string) error {
	return db.SaveNodeExchPattern(nodePatternName)
}

// Remove the node exchange pattern name from the local database.
func DeleteNodeExchPattern(db AgentDatabase) error {
	return db.DeleteNodeExchPattern()
}
