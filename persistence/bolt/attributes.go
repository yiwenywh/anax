package bolt

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/open-horizon/anax/agreementbot/persistence"
	"github.com/open-horizon/anax/policy"
)

func init() {   // TODO: is this the right place to do init?
	persistence.Register("bolt", new(AgentBoltDB))
}

// TOOD: is this the right place to define AgentBoltDB?
// This is the object that represents the handle to the bolt func (db *AgentBoltDB)
type AgentBoltDB struct {
	db *bolt.DB
}

func (db *AgentBoltDB) String() string {
	return fmt.Sprintf("DB Handle: %v", db.db)
}

// FindAttributeByKey is used to fetch a single attribute by its primary key
func (db *AgentBoltDB) FindAttributeByKey(attributeid string) (*Attribute, error) {
	var attr Attribute
	var bucket *bolt.Bucket

	readErr := db.db.View(func(tx *bolt.Tx) error {
		bucket = tx.Bucket([]byte(ATTRIBUTES))
		if bucket != nil {

			v := bucket.Get([]byte(id))
			if v != nil {
				var err error
				attr, err = persistence.HydrateConcreteAttribute(v)
				if err != nil {
					return err
				} else if attr == nil {
					return nil
				}
			}
		}

		return nil
	})

	if readErr != nil {
		if bucket == nil {
			// no bucket created yet so record not found
			return nil, nil
		}

		return nil, readErr
	}

	return &attr, nil
}

// get all the attribute that the given this service can use.
// If the given serviceUrl is an empty string, all attributes will be returned.
// For an attribute, if the a.ServiceSpecs is empty, it will be included.
// Otherwise, if an element in the attrubute's ServiceSpecs array equals to ServiceSpec{serviceUrl, org}
// the attribute will be included.
func (db *AgentBoltDB) FindApplicableAttributes(serviceUrl string, org string) ([]Attribute, error) {
	filteredAttrs := []Attribute{}

	return filteredAttrs, db.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ATTRIBUTES))

		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(k, v []byte) error {
			attr, err := persistence.HydrateConcreteAttribute(v)
			if err != nil {
				return err
			} else if attr != nil {
				serviceSpecs := persistence.GetAttributeServiceSpecs(&attr)
				if serviceSpecs == nil {
					filteredAttrs = append(filteredAttrs, attr)
				} else if serviceSpecs.SupportService(serviceUrl, org) {
					filteredAttrs = append(filteredAttrs, attr)
				}
			}
			return nil
		})
	})
}

func (db *AgentBoltDB) UpdateAttribute(attributeid string, attr *Attribute) (error) {
	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(ATTRIBUTES))
		if err != nil {
			return err
		}
		serial, err := json.Marshal(attr)
		if err != nil {
			return fmt.Errorf("Failed to serialize attribute: %v. Error: %v", attr, err)
		}
		return bucket.Put([]byte(attributeid), serial)
	})

	return writeErr
}

func (db *AgentBoltDB) DeleteAttribute(attributeid string) (*Attribute, error) {
	existing, err := db.FindAttributeByKey(id)
	if err != nil {
		return nil, fmt.Errorf("Failed to search for existing attribute: %v", err)
	}

	if *existing == nil {
		return nil, nil
	}

	delError := db.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(ATTRIBUTES))
		if err != nil {
			return err
		}
		return bucket.Delete([]byte(id))
	})

	return existing, delError
}
