package bolt

import (
	"encoding/json"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"github.com/open-horizon/anax/persistence"
)

func init() {   // TODO: is this the right place to do init?
	persistence.Register("bolt", new(AgentBoltDB))
}

// FindNodeStatus returns the node status currently in the local db
func (db *AgentBoltDB) FindNodeStatus() ([]persistence.WorkloadStatus, error) {
	var nodeStatus []persistence.WorkloadStatus

	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(persistence.NODE_STATUS)); b != nil {
			return b.ForEach(func(k, v []byte) error {

				if err := json.Unmarshal(v, &nodeStatus); err != nil {
					return fmt.Errorf("Unable to deserialize node status record: %v", v)
				}

				return nil
			})
		}

		return nil // end transaction
	})

	if readErr != nil {
		return nil, readErr
	}
	return nodeStatus, nil	
}

// SaveNodeStatus saves the provided node status to the local db
func (db *AgentBoltDB) SaveNodeStatus(status []persistence.WorkloadStatus) error {
	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(persistence.NODE_STATUS))
		if err != nil {
			return err
		}

		if serial, err := json.Marshal(status); err != nil {
			return fmt.Errorf("Failed to serialize node status: %v. Error: %v", status, err)
		} else {
			return b.Put([]byte(persistence.NODE_STATUS), serial)
		}
	})

	return writeErr
}

// DeleteSurfaceErrors delete node status from the local database
func (db *AgentBoltDB) DeleteNodeStatus() error {
	if seList, err := db.FindNodeStatus(); err != nil {
		return err
	} else if len(seList) == 0 {
		return nil
	} else {
		return db.db.Update(func(tx *bolt.Tx) error {

			if b, err := tx.CreateBucketIfNotExists([]byte(persistence.NODE_STATUS)); err != nil {
				return err
			} else if err := b.Delete([]byte(persistence.NODE_STATUS)); err != nil {
				return fmt.Errorf("Unable to delete node status object: %v", err)
			} else {
				return nil
			}
		})
	}	
}
