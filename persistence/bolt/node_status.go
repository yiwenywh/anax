package bolt

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	bolt "go.etcd.io/bbolt"
)

// FindNodeStatus returns the node status currently in the local db
func (db *AgentBoltDB) FindNodeStatus() ([]WorkloadStatus, error) {
	var nodeStatus []WorkloadStatus

	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(NODE_STATUS)); b != nil {
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
func (db *AgentBoltDB) SaveNodeStatus(status []WorkloadStatus) error {
	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(NODE_STATUS))
		if err != nil {
			return err
		}

		if serial, err := json.Marshal(status); err != nil {
			return fmt.Errorf("Failed to serialize node status: %v. Error: %v", status, err)
		} else {
			return b.Put([]byte(NODE_STATUS), serial)
		}
	})

	return writeErr
}

// DeleteSurfaceErrors delete node status from the local database
func (db *AgentBoltDB) DeleteNodeStatus() error {
	if seList, err := FindNodeStatus(); err != nil {
		return err
	} else if len(seList) == 0 {
		return nil
	} else {
		return db.db.Update(func(tx *bolt.Tx) error {

			if b, err := tx.CreateBucketIfNotExists([]byte(NODE_STATUS)); err != nil {
				return err
			} else if err := b.Delete([]byte(NODE_STATUS)); err != nil {
				return fmt.Errorf("Unable to delete node status object: %v", err)
			} else {
				return nil
			}
		})
	}	
}