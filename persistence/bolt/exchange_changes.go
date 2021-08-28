package bolt

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"time"
	bolt "go.etcd.io/bbolt"
)

func (db *AgentBoltDB) FindExchangeChangeState() (*ChangeState, error) {

	chg := make([]ChangeState, 0)

	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(EXCHANGE_CHANGES)); b != nil {
			return b.ForEach(func(k, v []byte) error {
				var c ChangeState

				if err := json.Unmarshal(v, &c); err != nil {
					return fmt.Errorf("Unable to deserialize exchange change state %v, error: %v", v, err)
				}

				chg = append(chg, c)
				return nil
			})
		}

		return nil // end transaction
	})

	if readErr != nil {
		return nil, readErr
	}

	glog.V(5).Infof("Demarshalled saved exchange change state: %v", chg)

	if len(chg) > 1 {
		return nil, fmt.Errorf("Unsupported db state: more than one change state stored in bucket: %v", chg)
	} else if len(chg) == 1 {
		return &chg[0], nil
	} else {
		return nil, nil
	}	
}

func (db *AgentBoltDB) SaveExchangeChangeState(changeID uint64) error {

	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(EXCHANGE_CHANGES))
		if err != nil {
			return err
		}

		chg := ChangeState{
			ChangeID:    changeID,
			LastUpdated: time.Now().Unix(),
		}

		if serial, err := json.Marshal(chg); err != nil {
			return fmt.Errorf("Failed to serialize change state %v, error: %v", chg, err)
		} else if err := b.Put([]byte(EXCHANGE_CHANGES), serial); err != nil {
			return fmt.Errorf("Failed to save change state %v, error: %v", chg, err)
		} else {
			glog.V(3).Infof("Successfully saved exchange change state: %v", chg)
			return nil
		}
	})

	return writeErr	
}

func (db *AgentBoltDB) DeleteExchangeChangeState() error {

	if chg, err := FindExchangeChangeState(); err != nil {
		return err
	} else if chg == nil {
		return nil
	} else {

		return db.db.Update(func(tx *bolt.Tx) error {

			if b, err := tx.CreateBucketIfNotExists([]byte(EXCHANGE_CHANGES)); err != nil {
				return err
			} else if err := b.Delete([]byte(EXCHANGE_CHANGES)); err != nil {
				return fmt.Errorf("Unable to delete exchange change state, error: %v", err)
			} else {
				return nil
			}
		})
	}	
}