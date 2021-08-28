package bolt

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	bolt "go.etcd.io/bbolt"
)

// save the given microserviceSecretStatus instance into the db. Key: MsInstKey, Value: MicroserviceSecretStatus Object
func (db *AgentBoltDB) SaveMSSInst(new_secret_status_inst *MicroserviceSecretStatusInst) (*MicroserviceSecretStatusInst, error) {
	return new_secret_status_inst, db.db.Update(func(tx *bolt.Tx) error {
		if b, err := tx.CreateBucketIfNotExists([]byte(SECRET_STATUS)); err != nil {
			return err
		} else if bytes, err := json.Marshal(new_secret_status_inst); err != nil {
			return fmt.Errorf("Unable to marshal new record: %v", err)
		} else if err := b.Put([]byte(new_secret_status_inst.MsInstKey), []byte(bytes)); err != nil {
			return fmt.Errorf("Unable to persist service instance: %v", err)
		}
		// success, close tx
		return nil
	})	
}

func (db *AgentBoltDB) FindMSSInstWithKey(ms_inst_key string) (*MicroserviceSecretStatusInst, error) {
	var pmsSecretStatusInst *MicroserviceSecretStatusInst
	pmsSecretStatusInst = nil

	// fetch microserviceSecretStatus instances
	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(SECRET_STATUS)); b != nil {
			v := b.Get([]byte(ms_inst_key))

			var msSecretStatusInst MicroserviceSecretStatusInst

			if err := json.Unmarshal(v, &msSecretStatusInst); err != nil {
				glog.Errorf("Unable to deserialize microserviceSecretStatus instance db record: %v. Error: %v", v, err)
				return err
			} else {
				pmsSecretStatusInst = &msSecretStatusInst
				return nil
			}
		}

		return nil // end the transaction
	})

	if readErr != nil {
		return nil, readErr
	} else {
		return pmsSecretStatusInst, nil
	}	
}

func (db *AgentBoltDB) FindMSSInstWithESSToken(ess_token string) (*MicroserviceSecretStatusInst, error) {
	var pms *MicroserviceSecretStatusInst
	pms = nil

	// fetch microserviceSecretStatus instances
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(SECRET_STATUS)); b != nil {
			cursor := b.Cursor()
			for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
				var msSecretStatusInstance MicroserviceSecretStatusInst
				if err := json.Unmarshal(value, &msSecretStatusInstance); err != nil {
					return err
				}

				if msSecretStatusInstance.ESSToken == ess_token {
					pms = &msSecretStatusInstance
					return nil
				}
			}
		}

		return nil // end the transaction
	})

	if readErr != nil {
		return nil, readErr
	} else {
		return pms, nil
	}	
}

// delete a microserviceSecretStatus instance from db. It will NOT return error if it does not exist in the db
func (db *AgentBoltDB) DeleteMSSInstWithKey(ms_inst_key string) (*MicroserviceSecretStatusInst, error) {
	if ms_inst_key == "" {
		return nil, errors.New("microserviceInstantKey (key) is empty, cannot remove")
	} else {
		if ms, err := FindMSSInstWithKey(ms_inst_key); err != nil {
			return nil, err
		} else if ms == nil {
			return nil, nil
		} else {
			return ms, db.db.Update(func(tx *bolt.Tx) error {
				if b, err := tx.CreateBucketIfNotExists([]byte(SECRET_STATUS)); err != nil {
					return err
				} else if err := b.Delete([]byte(ms_inst_key)); err != nil {
					return fmt.Errorf("Unable to delete microserviceSecretStatus instance %v: %v", ms_inst_key, err)
				} else {
					return nil
				}
			})
		}
	}	
}

func (db *AgentBoltDB) DeleteMSSInstWithESSToken(ess_token string) (*MicroserviceSecretStatusInst, error) {
	if ess_token == "" {
		return nil, errors.New("ess_token(key) is empty, cannot remove")
	} else {
		if ms, err := FindMSSInstWithESSToken(ess_token); err != nil {
			return nil, err
		} else if ms == nil {
			return nil, nil
		} else {
			return ms, db.db.Update(func(tx *bolt.Tx) error {
				if b, err := tx.CreateBucketIfNotExists([]byte(SECRET_STATUS)); err != nil {
					return err
				} else if err := b.Delete([]byte(ms.MsInstKey)); err != nil {
					return fmt.Errorf("Unable to delete microserviceSecretStatus instance with ess_token %v: %v", ess_token, err)
				} else {
					return nil
				}
			})
		}
	}	
}

func (db *AgentBoltDB) PersistUpdatedMSSInst(ms_inst_key string, update *MicroserviceSecretStatusInst) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		if b, err := tx.CreateBucketIfNotExists([]byte(SECRET_STATUS)); err != nil {
			return err
		} else {
			current := b.Get([]byte(ms_inst_key))
			var mod MicroserviceSecretStatusInst

			if current == nil {
				return fmt.Errorf("No service with given key available to update: %v", ms_inst_key)
			} else if err := json.Unmarshal(current, &mod); err != nil {
				return fmt.Errorf("Failed to unmarshal service DB data: %v. Error: %v", string(current), err)
			} else {

				// This code is running in a database transaction. Within the tx, the current record is
				// read and then updated according to the updates within the input update record. It is critical
				// to check for correct data transitions within the tx.
				mod.ESSToken = update.ESSToken
				mod.SecretsStatus = update.SecretsStatus

				if serialized, err := json.Marshal(mod); err != nil {
					return fmt.Errorf("Failed to serialize contract record: %v. Error: %v", mod, err)
				} else if err := b.Put([]byte(ms_inst_key), serialized); err != nil {
					return fmt.Errorf("Failed to write microserviceSecretStatus instance with key: %v. Error: %v", ms_inst_key, err)
				} else {
					glog.V(2).Infof("Succeeded updating microserviceSecretStatus instance record to %v", mod)
					return nil
				}
			}
		}
	})	
}