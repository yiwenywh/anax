package bolt

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"strconv"
	bolt "go.etcd.io/bbolt"
)

// save the microservice record. update if it already exists in the db
func (db *AgentBoltDB) SaveOrUpdateMicroserviceDef(msdef *MicroserviceDefinition) error {
	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		if bucket, err := tx.CreateBucketIfNotExists([]byte(MICROSERVICE_DEFINITIONS)); err != nil {
			return err
		} else if nextKey, err := bucket.NextSequence(); err != nil {
			return fmt.Errorf("Unable to get sequence key for new msdef %v. Error: %v", msdef, err)
		} else {
			strKey := strconv.FormatUint(nextKey, 10)
			msdef.Id = strKey

			glog.V(5).Infof("saving service definition %v to db", *msdef)

			serial, err := json.Marshal(*msdef)
			if err != nil {
				return fmt.Errorf("Failed to serialize service: %v. Error: %v", *msdef, err)
			}
			return bucket.Put([]byte(strKey), serial)
		}
	})

	return writeErr	
}

// find the microservice definition from the db
func (db *AgentBoltDB) FindMicroserviceDefWithKey(key string) (*MicroserviceDefinition, error) {
	var pms *MicroserviceDefinition
	pms = nil

	// fetch microservice definitions
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(MICROSERVICE_DEFINITIONS)); b != nil {
			v := b.Get([]byte(key))

			var ms MicroserviceDefinition

			if err := json.Unmarshal(v, &ms); err != nil {
				glog.Errorf("Unable to deserialize service definition db record: %v. Error: %v", v, err)
				return err
			} else {
				pms = &ms
				return nil
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

// find the microservice instance from the db
func (db *AgentBoltDB) FindMicroserviceDefs(filters []MSFilter) ([]MicroserviceDefinition, error) {
	ms_defs := make([]MicroserviceDefinition, 0)

	// fetch contracts
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(MICROSERVICE_DEFINITIONS)); b != nil {
			b.ForEach(func(k, v []byte) error {

				var e MicroserviceDefinition

				if err := json.Unmarshal(v, &e); err != nil {
					glog.Errorf("Unable to deserialize db record: %v", v)
				} else {
					glog.V(5).Infof("Demarshalled service definition in DB: %v", e.ShortString())
					exclude := false
					for _, filterFn := range filters {
						if !filterFn(e) {
							exclude = true
						}
					}
					if !exclude {
						ms_defs = append(ms_defs, e)
					}
				}
				return nil
			})
		}

		return nil // end the transaction
	})

	if readErr != nil {
		return nil, readErr
	} else {
		return ms_defs, nil
	}	
}

// find the microservice instance from the db
func (db *AgentBoltDB) FindMicroserviceInstance(url string, org string, version string, instance_id string) (*MicroserviceInstance, error) {
	var pms *MicroserviceInstance
	pms = nil

	// fetch microservice instances
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(MICROSERVICE_INSTANCES)); b != nil {
			b.ForEach(func(k, v []byte) error {

				var ms MicroserviceInstance

				if err := json.Unmarshal(v, &ms); err != nil {
					glog.Errorf("Unable to deserialize service_instance db record: %v", v)
				} else if ms.SpecRef == url && ms.Version == version && ms.InstanceId == instance_id {
					// ms.Org == "" is for ms instances created by older versions
					if ms.Org == "" || ms.Org == org {
						pms = &ms
					}
					return nil
				}
				return nil
			})
		}

		return nil // end the transaction
	})

	if readErr != nil {
		return nil, readErr
	} else {
		return pms, nil
	}	
}

// find the microservice instance from the db
func (db *AgentBoltDB) FindMicroserviceInstanceWithKey(key string) (*MicroserviceInstance, error) {
	var pms *MicroserviceInstance
	pms = nil

	// fetch microservice instances
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(MICROSERVICE_INSTANCES)); b != nil {
			v := b.Get([]byte(key))
			if v == nil {
				return nil
			}

			var ms MicroserviceInstance

			if err := json.Unmarshal(v, &ms); err != nil {
				glog.Errorf("Unable to deserialize service instance db record: %v. Error: %v", v, err)
				return err
			} else {
				pms = &ms
				return nil
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

// find the microservice instance from the db
func (db *AgentBoltDB) FindMicroserviceInstances(filters []MIFilter) ([]MicroserviceInstance, error) {
	ms_instances := make([]MicroserviceInstance, 0)

	// fetch contracts
	readErr := db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(MICROSERVICE_INSTANCES)); b != nil {
			b.ForEach(func(k, v []byte) error {

				var e MicroserviceInstance

				if err := json.Unmarshal(v, &e); err != nil {
					glog.Errorf("Unable to deserialize db record: %v", v)
				} else {
					glog.V(5).Infof("Demarshalled service instance in DB: %v", e)
					exclude := false
					for _, filterFn := range filters {
						if !filterFn(e) {
							exclude = true
						}
					}
					if !exclude {
						ms_instances = append(ms_instances, e)
					}
				}
				return nil
			})
		}

		return nil // end the transaction
	})

	if readErr != nil {
		return nil, readErr
	} else {
		return ms_instances, nil
	}	
}

// delete a microservice instance from db. It will NOT return error if it does not exist in the db
func (db *AgentBoltDB) DeleteMicroserviceInstance(key string) (*MicroserviceInstance, error) {

	if key == "" {
		return nil, errors.New("key is empty, cannot remove")
	} else {
		if ms, err := FindMicroserviceInstanceWithKey(key); err != nil {
			return nil, err
		} else if ms == nil {
			return nil, nil
		} else {
			return ms, db.db.Update(func(tx *bolt.Tx) error {

				if b, err := tx.CreateBucketIfNotExists([]byte(MICROSERVICE_INSTANCES)); err != nil {
					return err
				} else if err := b.Delete([]byte(key)); err != nil {
					return fmt.Errorf("Unable to delete service instance %v: %v", key, err)
				} else {
					return nil
				}
			})
		}
	}	
}

// does whole-member replacements of values that are legal to change
func (db *AgentBoltDB) PersistUpdatedMicroserviceDef(key string, update *MicroserviceDefinition) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		if b, err := tx.CreateBucketIfNotExists([]byte(MICROSERVICE_DEFINITIONS)); err != nil {
			return err
		} else {
			current := b.Get([]byte(key))
			var mod MicroserviceDefinition

			if current == nil {
				return fmt.Errorf("No service with given key available to update: %v", key)
			} else if err := json.Unmarshal(current, &mod); err != nil {
				return fmt.Errorf("Failed to unmarshal service DB data: %v. Error: %v", string(current), err)
			} else {

				// This code is running in a database transaction. Within the tx, the current record is
				// read and then updated according to the updates within the input update record. It is critical
				// to check for correct data transitions within the tx.
				if mod.UpgradeStartTime == 0 { // 1 transition from zero to non-zero
					mod.UpgradeStartTime = update.UpgradeStartTime
				}
				if mod.UpgradeMsUnregisteredTime == 0 {
					mod.UpgradeMsUnregisteredTime = update.UpgradeMsUnregisteredTime
				}
				if mod.UpgradeAgreementsClearedTime == 0 {
					mod.UpgradeAgreementsClearedTime = update.UpgradeAgreementsClearedTime
				}
				if mod.UpgradeExecutionStartTime == 0 {
					mod.UpgradeExecutionStartTime = update.UpgradeExecutionStartTime
				}
				if mod.UpgradeMsReregisteredTime == 0 {
					mod.UpgradeMsReregisteredTime = update.UpgradeMsReregisteredTime
				}
				if mod.UpgradeFailedTime == 0 {
					mod.UpgradeFailedTime = update.UpgradeFailedTime
				}
				if mod.UngradeFailureReason == 0 {
					mod.UngradeFailureReason = update.UngradeFailureReason
				}
				if mod.UngradeFailureDescription == "" {
					mod.UngradeFailureDescription = update.UngradeFailureDescription
				}

				if mod.Archived != update.Archived {
					mod.Archived = update.Archived
				}

				if mod.UpgradeNewMsId != update.UpgradeNewMsId {
					mod.UpgradeNewMsId = update.UpgradeNewMsId
				}

				if mod.UpgradeVersionRange != update.UpgradeVersionRange {
					mod.UpgradeVersionRange = update.UpgradeVersionRange
				}

				if serialized, err := json.Marshal(mod); err != nil {
					return fmt.Errorf("Failed to serialize contract record: %v. Error: %v", mod, err)
				} else if err := b.Put([]byte(key), serialized); err != nil {
					return fmt.Errorf("Failed to write service definition %v version %v key %v. Error: %v", mod.SpecRef, mod.Version, key, err)
				} else {
					glog.V(2).Infof("Succeeded updating service definition record to %v", mod.ShortString())
					return nil
				}
			}
		}
	})	
}

// does whole-member replacements of values that are legal to change
func (db *AgentBoltDB) PersistUpdatedMicroserviceInstance(key string, update *MicroserviceInstance) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		if b, err := tx.CreateBucketIfNotExists([]byte(MICROSERVICE_INSTANCES)); err != nil {
			return err
		} else {
			current := b.Get([]byte(key))
			var mod MicroserviceInstance

			if current == nil {
				return fmt.Errorf("No service with given key available to update: %v", key)
			} else if err := json.Unmarshal(current, &mod); err != nil {
				return fmt.Errorf("Failed to unmarshal service DB data: %v. Error: %v", string(current), err)
			} else {

				// This code is running in a database transaction. Within the tx, the current record is
				// read and then updated according to the updates within the input update record. It is critical
				// to check for correct data transitions within the tx.
				if !mod.Archived { // 1 transition from false to true
					mod.Archived = update.Archived
				}
				if mod.InstanceCreationTime == 0 { // 1 transition from zero to non-zero
					mod.InstanceCreationTime = update.InstanceCreationTime
				}
				mod.ExecutionStartTime = update.ExecutionStartTime
				mod.ExecutionFailureCode = update.ExecutionFailureCode
				mod.ExecutionFailureDesc = update.ExecutionFailureDesc
				mod.CleanupStartTime = update.CleanupStartTime
				mod.AssociatedAgreements = update.AssociatedAgreements
				mod.RetryStartTime = update.RetryStartTime
				mod.MaxRetries = update.MaxRetries
				mod.MaxRetryDuration = update.MaxRetryDuration
				mod.CurrentRetryCount = update.CurrentRetryCount
				mod.EnvVars = update.EnvVars

				if len(mod.ParentPath) != len(update.ParentPath) {
					mod.ParentPath = update.ParentPath
				}

				if !mod.AgreementLess { // 1 transition from false to true
					mod.AgreementLess = update.AgreementLess
				}

				if serialized, err := json.Marshal(mod); err != nil {
					return fmt.Errorf("Failed to serialize contract record: %v. Error: %v", mod, err)
				} else if err := b.Put([]byte(key), serialized); err != nil {
					return fmt.Errorf("Failed to write service instance with key: %v. Error: %v", key, err)
				} else {
					glog.V(2).Infof("Succeeded updating service instance record to %v", mod)
					return nil
				}
			}
		}
	})	
}

// save the given microservice instance into the db
func (db *AgentBoltDB) SaveMicroserviceInstance(new_inst *MicroserviceInstance) (*MicroserviceInstance, error) {
	return new_inst, db.db.Update(func(tx *bolt.Tx) error {
		if b, err := tx.CreateBucketIfNotExists([]byte(MICROSERVICE_INSTANCES)); err != nil {
			return err
		} else if bytes, err := json.Marshal(new_inst); err != nil {
			return fmt.Errorf("Unable to marshal new record: %v", err)
		} else if err := b.Put([]byte(new_inst.GetKey()), []byte(bytes)); err != nil {
			return fmt.Errorf("Unable to persist service instance: %v", err)
		}
		// success, close tx
		return nil
	})
}