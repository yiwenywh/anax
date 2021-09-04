package bolt

import (
	"encoding/json"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"github.com/open-horizon/anax/externalpolicy"
	"github.com/open-horizon/anax/persistence"
)

func init() {   // TODO: is this the right place to do init?
	persistence.Register("bolt", new(AgentBoltDB))
}

// Retrieve the node policy object from the database. The bolt APIs assume there is more than 1 object in a bucket,
// so this function has to be prepared for that case, even though there should only ever be 1.
func (db *AgentBoltDB) FindNodePolicy() (*externalpolicy.ExternalPolicy, error) {

	policy := make([]externalpolicy.ExternalPolicy, 0)

	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(persistence.NODE_POLICY)); b != nil {
			return b.ForEach(func(k, v []byte) error {
				var pol externalpolicy.ExternalPolicy

				if err := json.Unmarshal(v, &pol); err != nil {
					return fmt.Errorf("Unable to deserialize node policy record: %v", v)
				}

				policy = append(policy, pol)
				return nil
			})
		}

		return nil // end transaction
	})

	if readErr != nil {
		return nil, readErr
	}

	if len(policy) > 1 {
		return nil, fmt.Errorf("Unsupported db state: more than one node policy stored in bucket. Policies: %v", policy)
	} else if len(policy) == 1 {
		return &policy[0], nil
	} else {
		return nil, nil
	}	
}

// There is only 1 object in the bucket so we can use the bucket name as the object key.
func (db *AgentBoltDB) SaveNodePolicy(nodePolicy *externalpolicy.ExternalPolicy) error {

	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(persistence.NODE_POLICY))
		if err != nil {
			return err
		}

		if serial, err := json.Marshal(nodePolicy); err != nil {
			return fmt.Errorf("Failed to serialize node policy: %v. Error: %v", nodePolicy, err)
		} else {
			return b.Put([]byte(persistence.NODE_POLICY), serial)
		}
	})

	return writeErr	
}

// Remove the node policy object from the local database.
func (db *AgentBoltDB) DeleteNodePolicy() error {

	if pol, err := db.FindNodePolicy(); err != nil {
		return err
	} else if pol == nil {
		return nil
	} else {

		return db.db.Update(func(tx *bolt.Tx) error {

			if b, err := tx.CreateBucketIfNotExists([]byte(persistence.NODE_POLICY)); err != nil {
				return err
			} else if err := b.Delete([]byte(persistence.NODE_POLICY)); err != nil {
				return fmt.Errorf("Unable to delete node policy object: %v", err)
			} else {
				return nil
			}
		})
	}	
}

// Retrieve the exchange node policy lastUpdated string from the database.
func (db *AgentBoltDB) GetNodePolicyLastUpdated_Exch() (string, error) {

	lastUpdated := ""

	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(persistence.EXCHANGE_NP_LAST_UPDATED)); b != nil {
			return b.ForEach(func(k, v []byte) error {
				lastUpdated = string(v)
				return nil
			})
		}

		return nil // end transaction
	})

	if readErr != nil {
		return "", readErr
	}

	return lastUpdated, nil	
}

// save the exchange node policy lastUpdated string.
func (db *AgentBoltDB) SaveNodePolicyLastUpdated_Exch(lastUpdated string) error {

	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(persistence.EXCHANGE_NP_LAST_UPDATED))
		if err != nil {
			return err
		}

		return b.Put([]byte(persistence.EXCHANGE_NP_LAST_UPDATED), []byte(lastUpdated))

	})

	return writeErr	
}

// Remove the exchange node policy lastUpdated string from the local database.
func (db *AgentBoltDB) DeleteNodePolicyLastUpdated_Exch() error {

	if lastUpdated, err := db.GetNodePolicyLastUpdated_Exch(); err != nil {
		return err
	} else if lastUpdated == "" {
		return nil
	} else {
		return db.db.Update(func(tx *bolt.Tx) error {

			if b, err := tx.CreateBucketIfNotExists([]byte(persistence.EXCHANGE_NP_LAST_UPDATED)); err != nil {
				return err
			} else if err := b.Delete([]byte(persistence.EXCHANGE_NP_LAST_UPDATED)); err != nil {
				return fmt.Errorf("Unable to delete exchange node policy last updated string: %v", err)
			} else {
				return nil
			}
		})
	}	
}
