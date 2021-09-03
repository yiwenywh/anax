package persistence

import (
	"github.com/open-horizon/anax/externalpolicy"
)

// Constants used throughout the code.
const NODE_POLICY = "nodepolicy"                                   // The bucket name in the bolt DB.
const EXCHANGE_NP_LAST_UPDATED = "exchange_nodepolicy_lastupdated" // The buucket for the exchange last updated string

// Retrieve the node policy object from the database. The bolt APIs assume there is more than 1 object in a bucket,
// so this function has to be prepared for that case, even though there should only ever be 1.
func FindNodePolicy(db AgentDatabase) (*externalpolicy.ExternalPolicy, error) {
	return db.FindNodePolicy()
}

// There is only 1 object in the bucket so we can use the bucket name as the object key.
func SaveNodePolicy(db AgentDatabase, nodePolicy *externalpolicy.ExternalPolicy) error {
	return db.SaveNodePolicy(nodePolicy)
}

// Remove the node policy object from the local database.
func DeleteNodePolicy(db AgentDatabase) error {
	return db.DeleteNodePolicy()
}

// Retrieve the exchange node policy lastUpdated string from the database.
func GetNodePolicyLastUpdated_Exch(db AgentDatabase) (string, error) {
	return db.GetNodePolicyLastUpdated_Exch()
}

// save the exchange node policy lastUpdated string.
func SaveNodePolicyLastUpdated_Exch(db AgentDatabase, lastUpdated string) error {
	return db.SaveNodePolicyLastUpdated_Exch(lastUpdated)
}

// Remove the exchange node policy lastUpdated string from the local database.
func DeleteNodePolicyLastUpdated_Exch(db AgentDatabase) error {
	return db.DeleteNodePolicyLastUpdated_Exch()
}
