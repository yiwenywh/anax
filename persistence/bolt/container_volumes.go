package bolt

import (
	"encoding/json"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"github.com/golang/glog"
	"strconv"
)

func (db *AgentBoltDB) SaveContainerVolume(container_volume *ContainerVolume) error {
	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		if bucket, err := tx.CreateBucketIfNotExists([]byte(CONTAINER_VOLUMES)); err != nil {
			return err
		} else {
			// use the old key if it has one, otherwise generate one
			key := container_volume.RecordId
			if key == "" {
				if nextKey, err := bucket.NextSequence(); err != nil {
					return fmt.Errorf("Unable to get sequence key for saving new container volume %v. Error: %v", container_volume, err)
				} else {
					key = strconv.FormatUint(nextKey, 10)
					container_volume.RecordId = key
				}
			}

			serial, err := json.Marshal(*container_volume)
			if err != nil {
				return fmt.Errorf("Failed to serialize the container volume object: %v. Error: %v", *container_volume, err)
			}
			return bucket.Put([]byte(key), serial)
		}
	})

	return writeErr
}

func (db *AgentBoltDB) FindContainerVolumes(filters []ContainerVolumeFilter) ([]ContainerVolume, error) {
	cvs := make([]ContainerVolume, 0)

	// fetch container volumes
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(CONTAINER_VOLUMES)); b != nil {
			b.ForEach(func(k, v []byte) error {

				var cv ContainerVolume

				if err := json.Unmarshal(v, &cv); err != nil {
					glog.Errorf("Unable to deserialize ContainerVolume db record: %v. Error: %v", v, err)
				} else {
					exclude := false
					for _, filterFn := range filters {
						if !filterFn(cv) {
							exclude = true
						}
					}
					if !exclude {
						cvs = append(cvs, cv)
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
		return cvs, nil
	}
}