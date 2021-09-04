package bolt

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"time"
	bolt "go.etcd.io/bbolt"
	"github.com/open-horizon/anax/persistence"
)

func init() {   // TODO: is this the right place to do init?
	persistence.Register("bolt", new(AgentBoltDB))
}

func (db *AgentBoltDB) UpdateExchangeDevice(self *persistence.ExchangeDevice, deviceId string, invalidateToken bool, fn func(d persistence.ExchangeDevice) *persistence.ExchangeDevice) (*persistence.ExchangeDevice, error) {
	if deviceId == "" {
		return nil, fmt.Errorf("Illegal arguments specified.")
	}

	update := fn(*self)

	var mod persistence.ExchangeDevice

	return &mod, db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(persistence.DEVICES))
		if err != nil {
			return err
		}

		// b/c it's only possible to save one device in the bucket, we use "DEVICES" as the key name
		current := b.Get([]byte(persistence.DEVICES))

		if current == nil {
			return fmt.Errorf("No device with given device id to update: %v", deviceId)
		} else if err := json.Unmarshal(current, &mod); err != nil {
			return fmt.Errorf("Failed to unmarshal device data: %v. Error: %v", string(current), err)
		} else {

			// Even though there is only one key in the bucket, make sure the update is for the right device
			if mod.Id != deviceId {
				return fmt.Errorf("No device with given device id to update: %v", deviceId)
			}

			// Differentiate token invalidation from updating a token.
			if invalidateToken {
				mod.Token = ""
				mod.TokenValid = false

			} else if update.Token != mod.Token && update.Token != "" {
				mod.Token = update.Token
				mod.TokenValid = true
				mod.TokenLastValidTime = uint64(time.Now().Unix())
			}

			// Write updates only to the fields we expect should be updateable
			if mod.Config.State != update.Config.State {
				mod.Config.State = update.Config.State
				mod.Config.LastUpdateTime = update.Config.LastUpdateTime
			}

			// Update the node type
			if mod.NodeType != update.NodeType {
				mod.NodeType = update.NodeType
			}

			// Update the pattern
			if mod.Pattern != update.Pattern {
				mod.Pattern = update.Pattern
			}

			// note: DEVICES is used as the key b/c we only want to store one value in this bucket

			if serialized, err := json.Marshal(mod); err != nil {
				return fmt.Errorf("Failed to serialize device record: %v. Error: %v", mod, err)
			} else if err := b.Put([]byte(persistence.DEVICES), serialized); err != nil {
				return fmt.Errorf("Failed to write device record with key: %v. Error: %v", persistence.DEVICES, err)
			} else {
				glog.V(2).Infof("Succeeded updating device record to %v", mod)
				return nil
			}
		}
	})
}

func (db *AgentBoltDB) SaveNewExchangeDevice (id string, token string, name string, nodeType string, ha bool, organization string, pattern string, configstate string) (*persistence.ExchangeDevice, error) {
	if id == "" || token == "" || name == "" || organization == "" || configstate == "" {
		return nil, errors.New("Argument null and must not be")
	}

	duplicate := false

	dErr := db.db.View(func(tx *bolt.Tx) error {
		bd := tx.Bucket([]byte(persistence.DEVICES))
		if bd != nil {
			duplicate = (bd.Get([]byte(name)) != nil)
		}

		return nil

	})

	if dErr != nil {
		return nil, fmt.Errorf("Error checking duplicates of device named %v from db. Error: %v", name, dErr)
	} else if duplicate {
		return nil, fmt.Errorf("Duplicate record found in devices for %v.", name)
	}

	exDevice, err := newExchangeDevice(id, token, name, nodeType, uint64(time.Now().Unix()), ha, organization, pattern, configstate)

	if err != nil {
		return nil, err
	}

	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(persistence.DEVICES))
		if err != nil {
			return err
		}

		// note: DEVICES is used as the key b/c we only want to store one value in this bucket

		if serial, err := json.Marshal(&exDevice); err != nil {
			return fmt.Errorf("Failed to serialize device: %v. Error: %v", exDevice, err)
		} else {
			return b.Put([]byte(persistence.DEVICES), serial)
		}
	})

	return exDevice, writeErr
}

func (db *AgentBoltDB) FindExchangeDevice() (*persistence.ExchangeDevice, error) {
	devices := make([]persistence.ExchangeDevice, 0)

	readErr := db.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(persistence.DEVICES)); b != nil {
			return b.ForEach(func(k, v []byte) error {
				var dev persistence.ExchangeDevice

				if err := json.Unmarshal(v, &dev); err != nil {
					return fmt.Errorf("Unable to deserializer db record: %v", v)
				}

				devices = append(devices, dev)
				return nil
			})
		}

		return nil // end transaction
	})

	if readErr != nil {
		return nil, readErr
	}

	if len(devices) > 1 {
		return nil, fmt.Errorf("Unsupported state: more than one exchange device stored in bucket. Devices: %v", devices)
	} else if len(devices) == 1 {
		// convert the pattern string to standard "org/pattern" format.
		if devices[0].Pattern != "" {
			_, _, pattern := persistence.GetFormatedPatternString(devices[0].Pattern, devices[0].Org)
			devices[0].Pattern = pattern
		}

		if devices[0].NodeType == "" {
			devices[0].NodeType = persistence.DEVICE_TYPE_DEVICE
		}
		return &devices[0], nil
	} else {
		return nil, nil
	}
}

func (db *AgentBoltDB) DeleteExchangeDevice() error {
	if dev, err := db.FindExchangeDevice(); err != nil {
		return err
	} else if dev == nil {
		return fmt.Errorf("could not find record for device")
	} else {

		return db.db.Update(func(tx *bolt.Tx) error {

			if b, err := tx.CreateBucketIfNotExists([]byte(persistence.DEVICES)); err != nil {
				return err
			} else if err := b.Delete([]byte(persistence.DEVICES)); err != nil {
				return fmt.Errorf("Unable to delete horizon device object: %v", err)
			} else {
				return nil
			}
		})
	}
}

func newExchangeDevice(id string, token string, name string, nodeType string, tokenLastValidTime uint64, ha bool, org string, pattern string, configstate string) (*persistence.ExchangeDevice, error) {
	if id == "" || token == "" || name == "" || tokenLastValidTime == 0 || org == "" {
		return nil, errors.New("Cannot create exchange device, illegal arguments")
	}

	cfg := persistence.Configstate{
		State:          configstate,
		LastUpdateTime: uint64(time.Now().Unix()),
	}

	// make the pattern to the standard "org/pattern" format
	if pattern != "" {
		_, _, pat := persistence.GetFormatedPatternString(pattern, org)
		pattern = pat
	}

	return &persistence.ExchangeDevice{
		Id:                 id,
		Name:               name,
		NodeType:           nodeType,
		Token:              token,
		TokenLastValidTime: tokenLastValidTime,
		TokenValid:         true,
		HA:                 ha,
		Org:                org,
		Pattern:            pattern,
		Config:             cfg,
	}, nil
}
