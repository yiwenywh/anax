package bolt

import (
	"encoding/json" // ??
	"fmt"
	"github.com/golang/glog" // ??
	bolt "go.etcd.io/bbolt"
)

// Retrieve the node exchange pattern name from the database. It is set when the exchange node pattern is different
// from the local registered node pattern. It will be cleared once the device pattern get changed.
// The bolt APIs assume there is more than 1 object in a bucket,
// so this function has to be prepared for that case, even though there should only ever be 1.
func (db *AgentBoltDB) FindSavedNodeExchPattern() (string, error) {

	pattern_name := ""

	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(NODE_EXCH_PATTERN)); b != nil {
			return b.ForEach(func(k, v []byte) error {
				pattern_name = string(v)
				return nil
			})
		}

		return nil // end transaction
	})

	if readErr != nil {
		return "", readErr
	}

	return pattern_name, nil
}

// There is only 1 object in the bucket so we can use the bucket name as the object key.
func (db *AgentBoltDB) SaveNodeExchPattern(nodePatternName string) error {

	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(NODE_EXCH_PATTERN))
		if err != nil {
			return err
		}

		return b.Put([]byte(NODE_EXCH_PATTERN), []byte(nodePatternName))

	})

	return writeErr	
}

// Remove the node exchange pattern name from the local database.
func (db *AgentBoltDB) DeleteNodeExchPattern() error {

	if pattern_name, err := FindSavedNodeExchPattern(); err != nil {
		return err
	} else if pattern_name == "" {
		return nil
	} else {

		return db.db.Update(func(tx *bolt.Tx) error {

			if b, err := tx.CreateBucketIfNotExists([]byte(NODE_EXCH_PATTERN)); err != nil {
				return err
			} else if err := b.Delete([]byte(NODE_EXCH_PATTERN)); err != nil {
				return fmt.Errorf("Unable to delete node pattern from local db: %v", err)
			} else {
				return nil
			}
		})
	}	
}