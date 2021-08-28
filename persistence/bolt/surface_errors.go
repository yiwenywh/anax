package bolt

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	bolt "go.etcd.io/bbolt"
)

// FindSurfaceErrors returns the surface errors currently in the local db
func (db *AgentBoltDB) FindSurfaceErrors() ([]SurfaceError, error) {
	var surfaceErrors []SurfaceError

	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(NODE_SURFACEERR)); b != nil {
			return b.ForEach(func(k, v []byte) error {

				if err := json.Unmarshal(v, &surfaceErrors); err != nil {
					return fmt.Errorf("Unable to deserialize node surface error record: %v", v)
				}

				return nil
			})
		}

		return nil // end transaction
	})

	if readErr != nil {
		return nil, readErr
	}
	return surfaceErrors, nil
}

// SaveSurfaceErrors saves the provided list of surface errors to the local db
func (db *AgentBoltDB) SaveSurfaceErrors(surfaceErrors []SurfaceError) error {
	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(NODE_SURFACEERR))
		if err != nil {
			return err
		}

		if serial, err := json.Marshal(surfaceErrors); err != nil {
			return fmt.Errorf("Failed to serialize surface errors: %v. Error: %v", surfaceErrors, err)
		} else {
			return b.Put([]byte(NODE_SURFACEERR), serial)
		}
	})

	return writeErr
}

// DeleteSurfaceErrors delete node surface errors from the local database
func (db *AgentBoltDB) DeleteSurfaceErrors() error {
	if seList, err := FindSurfaceErrors(); err != nil {
		return err
	} else if len(seList) == 0 {
		return nil
	} else {
		return db.db.Update(func(tx *bolt.Tx) error {

			if b, err := tx.CreateBucketIfNotExists([]byte(NODE_SURFACEERR)); err != nil {
				return err
			} else if err := b.Delete([]byte(NODE_SURFACEERR)); err != nil {
				return fmt.Errorf("Unable to delete node surface error object: %v", err)
			} else {
				return nil
			}
		})
	}
}