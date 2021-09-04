package bolt

import (
	"encoding/json"
	"fmt"
	"github.com/open-horizon/anax/policy"
	"github.com/open-horizon/anax/persistence"
	bolt "go.etcd.io/bbolt"
)

func init() {   // TODO: is this the right place to do init?
	persistence.Register("bolt", new(AgentBoltDB))
}

// Retrieve the node user input object from the database. The bolt APIs assume there is more than 1 object in a bucket,
// so this function has to be prepared for that case, even though there should only ever be 1.
func (db *AgentBoltDB) FindNodeUserInput() ([]policy.UserInput, error) {

	var userInput []policy.UserInput

	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(persistence.NODE_USERINPUT)); b != nil {
			return b.ForEach(func(k, v []byte) error {

				if err := json.Unmarshal(v, &userInput); err != nil {
					return fmt.Errorf("Unable to deserialize node user input record: %v", v)
				}

				return nil
			})
		}

		return nil // end transaction
	})

	if readErr != nil {
		return nil, readErr
	}

	return userInput, nil
}

// There is only 1 object in the bucket so we can use the bucket name as the object key.
func (db *AgentBoltDB) SaveNodeUserInput(userInput []policy.UserInput) error {

	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(persistence.NODE_USERINPUT))
		if err != nil {
			return err
		}

		if serial, err := json.Marshal(userInput); err != nil {
			return fmt.Errorf("Failed to serialize node user input: %v. Error: %v", userInput, err)
		} else {
			return b.Put([]byte(persistence.NODE_USERINPUT), serial)
		}
	})

	return writeErr	
}

// Remove the node user input object from the local database.
func (db *AgentBoltDB) DeleteNodeUserInput() error {

	if ui, err := db.FindNodeUserInput(); err != nil {
		return err
	} else if ui == nil {
		return nil
	} else {

		return db.db.Update(func(tx *bolt.Tx) error {

			if b, err := tx.CreateBucketIfNotExists([]byte(persistence.NODE_USERINPUT)); err != nil {
				return err
			} else if err := b.Delete([]byte(persistence.NODE_USERINPUT)); err != nil {
				return fmt.Errorf("Unable to delete node user input object: %v", err)
			} else {
				return nil
			}
		})
	}	
}

// Retrieve the exchange node user input hash from the database.
func (db *AgentBoltDB) GetNodeUserInputHash_Exch() ([]byte, error) {

	userInputHash := []byte{}

	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(persistence.EXCHANGE_NODE_USERINPUT_HASH)); b != nil {
			return b.ForEach(func(k, v []byte) error {
				userInputHash = v
				return nil
			})
		}

		return nil // end transaction
	})

	if readErr != nil {
		return nil, readErr
	}

	return userInputHash, nil	
}

// save the exchange node user input hash.
func (db *AgentBoltDB) SaveNodeUserInputHash_Exch(userInputHash []byte) error {

	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(persistence.EXCHANGE_NODE_USERINPUT_HASH))
		if err != nil {
			return err
		}

		return b.Put([]byte(persistence.EXCHANGE_NODE_USERINPUT_HASH), userInputHash)

	})

	return writeErr
}

// Remove the exchange node user input hash from the local database.
func (db *AgentBoltDB) DeleteNodeUserInputHash_Exch() error {

	if userInputHash, err := db.GetNodeUserInputHash_Exch(); err != nil {
		return err
	} else if userInputHash == nil || len(userInputHash) == 0 {
		return nil
	} else {
		return db.db.Update(func(tx *bolt.Tx) error {

			if b, err := tx.CreateBucketIfNotExists([]byte(persistence.EXCHANGE_NODE_USERINPUT_HASH)); err != nil {
				return err
			} else if err := b.Delete([]byte(persistence.EXCHANGE_NODE_USERINPUT_HASH)); err != nil {
				return fmt.Errorf("Unable to delete exchange node user input hash from local db: %v", err)
			} else {
				return nil
			}
		})
	}
}
