package bolt

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	bolt "go.etcd.io/bbolt"
	"github.com/open-horizon/anax/persistence"
)

func init() {   // TODO: is this the right place to do init?
	persistence.Register("bolt", new(AgentBoltDB))
}

// Save the secret bindings from an agreement
// This bucket is used to keep the secret information until such time that the microservice instance id is created
// After that id exists, the secrets will be saved in the SECRETS bucket keyed by ms instance id
func (db *AgentBoltDB) SaveAgreementSecrets(agId string, secretsList *[]persistence.PersistedServiceSecret) error {
	if secretsList == nil {
		return nil
	}

	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(persistence.AGREEMENT_SECRETS))
		if err != nil {
			return err
		}

		if serial, err := json.Marshal(secretsList); err != nil {
			return fmt.Errorf("Failed to serialize agreement secrets list: Error: %v", err)
		} else {
			return bucket.Put([]byte(agId), serial)
		}
	})

	return writeErr	
}

func (db *AgentBoltDB) FindAgreementSecrets(agId string) (*[]persistence.PersistedServiceSecret, error) {
	if db == nil {
		return nil, nil
	}

	var psecretRec *[]persistence.PersistedServiceSecret
	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(persistence.AGREEMENT_SECRETS)); b != nil {
			s := b.Get([]byte(agId))
			if s != nil {
				secretRec := []persistence.PersistedServiceSecret{}
				if err := json.Unmarshal(s, &secretRec); err != nil {
					glog.Errorf("Unable to deserialize agreement secret db record: %v. Error: %v", agId, err)
					return err
				} else {
					psecretRec = &secretRec
				}
			}
		}
		return nil
	})
	return psecretRec, readErr	
}

func (db *AgentBoltDB) DeleteAgreementSecrets(agId string) error {
	if db == nil {
		return nil
	}

	if agSecrets, err := db.FindAgreementSecrets(agId); err != nil {
		return err
	} else if agSecrets == nil {
		return nil
	} else {
		return db.db.Update(func(tx *bolt.Tx) error {

			if b, err := tx.CreateBucketIfNotExists([]byte(persistence.AGREEMENT_SECRETS)); err != nil {
				return err
			} else if err := b.Delete([]byte(agId)); err != nil {
				return fmt.Errorf("Unable to delete agreement secrets object: %v", err)
			} else {
				return nil
			}
		})
	}	
}

func (db *AgentBoltDB) SaveAllSecretsForService(msInstId string, secretToSaveAll *persistence.PersistedServiceSecrets) error {
	if db == nil {
		return nil
	}
	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(persistence.SECRETS))
		if err != nil {
			return err
		}

		if serial, err := json.Marshal(secretToSaveAll); err != nil {
			return fmt.Errorf("Failed to serialize secrets: Error: %v", err)
		} else {
			return bucket.Put([]byte(msInstId), serial)
		}
	})

	return writeErr	
}

// Gets the secret from the database, no error returned if none is found in the db
func (db *AgentBoltDB) FindAllSecretsForMS(msInstId string) (*persistence.PersistedServiceSecrets, error) {
	if db == nil {
		return nil, nil
	}
	var psecretRec *persistence.PersistedServiceSecrets
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(persistence.SECRETS)); b != nil {
			s := b.Get([]byte(msInstId))
			if s != nil {
				secretRec := persistence.PersistedServiceSecrets{}
				if err := json.Unmarshal(s, &secretRec); err != nil {
					glog.Errorf("Unable to deserialize service secret db record: %v. Error: %v", msInstId, err)
					return err
				} else {
					psecretRec = &secretRec
				}
			}
		}
		return nil
	})

	if readErr != nil {
		return nil, readErr
	} else {
		return psecretRec, nil
	}	
}

func (db *AgentBoltDB) FindAllServiceSecretsWithFilters(filters []persistence.SecFilter) ([]persistence.PersistedServiceSecrets, error) {
	matchingSecrets := []persistence.PersistedServiceSecrets{}
	if db == nil {
		return matchingSecrets, nil
	}

	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(persistence.SECRETS)); b != nil {
			b.ForEach(func(k, v []byte) error {
				var s persistence.PersistedServiceSecrets

				if err := json.Unmarshal(v, &s); err != nil {
					glog.Errorf("Unable to deserialize db record: %v", v)
				} else {
					exclude := false

					for _, filterFn := range filters {
						if !filterFn(s) {
							exclude = true
						}
					}

					if !exclude {
						matchingSecrets = append(matchingSecrets, s)
					}
				}
				return nil
			})
			return nil
		}
		return nil
	})

	if readErr != nil {
		return nil, readErr
	} else {
		return matchingSecrets, nil
	}
}

// Returns the secret from the db if it was there. No error returned if it is not in the db
func (db *AgentBoltDB) DeleteSecrets(secName string, msInstId string) (*persistence.PersistedServiceSecret, error) {
	if db == nil {
		return nil, nil
	}

	if allSec, err := db.FindAllSecretsForMS(msInstId); err != nil {
		return nil, err
	} else if allSec != nil {
		if _, ok := allSec.SecretsMap[secName]; ok {
			delete(allSec.SecretsMap, secName)
		}
		if len(allSec.SecretsMap) == 0 {
			retSec := allSec.SecretsMap[secName]
			return retSec, db.db.Update(func(tx *bolt.Tx) error {
				if b, err := tx.CreateBucketIfNotExists([]byte(persistence.SECRETS)); err != nil {
					return err
				} else if err := b.Delete([]byte(msInstId)); err != nil {
					return fmt.Errorf("Unable to delete secret %v for microservice def %v: %v", secName, msInstId, err)
				} else {
					return nil
				}
			})
		} else if err = db.SaveAllSecretsForService(msInstId, allSec); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
