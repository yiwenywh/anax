package persistence

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"strconv"
	"time"
)

// container volume table name
const CONTAINER_VOLUMES = "container_volumes"

type ContainerVolume struct {
	RecordId     string `json:"record_id"` // unique primary key for records
	Name         string `json:"name"`
	CreationTime uint64 `json:"creation_time"`
	ArchiveTime  uint64 `json:"archive_time"`
}

func NewContainerVolume(name string) *ContainerVolume {
	return &ContainerVolume{
		Name:         name,
		CreationTime: uint64(time.Now().Unix()),
		ArchiveTime:  0,
	}
}

func (w ContainerVolume) String() string {
	return fmt.Sprintf("RecordId: %v, "+
		"Name: %v, "+
		"CreationTime: %v, "+
		"ArchiveTime: %v",
		w.RecordId, w.Name, w.CreationTime, w.ArchiveTime)
}

func (w ContainerVolume) ShortString() string {
	return w.String()
}

// save the ContainerVolume record into db.
func SaveContainerVolume(db AgentDatabase, container_volume *ContainerVolume) error {
	return db.SaveContainerVolume(container_volume)
}

// save the container volume into db.
func SaveContainerVolumeByName(db AgentDatabase, name string) error {
	pcv := NewContainerVolume(name)
	return SaveContainerVolume(db, pcv)
}

// Find the container volumes that are not deleted yet
func FindAllUndeletedContainerVolumes(db AgentDatabase) ([]ContainerVolume, error) {
	return FindContainerVolumes(db, []ContainerVolumeFilter{UnarchivedCVFilter()})
}

// Mark the given volume as archived.
func ArchiveContainerVolumes(db AgentDatabase, cv *ContainerVolume) error {
	if cv == nil {
		return nil
	}
	cv.ArchiveTime = uint64(time.Now().Unix())
	if err := SaveContainerVolume(db, cv); err != nil {
		return fmt.Errorf("Failed to archive the container volume %v. %v", cv.Name, err)
	}
	return nil
}

// filter on ContainerVolume
type ContainerVolumeFilter func(ContainerVolume) bool

// filter on ArchiveTime time
func UnarchivedCVFilter() ContainerVolumeFilter {
	return func(c ContainerVolume) bool { return c.ArchiveTime == 0 }
}

// filter on name
func NameCVFilter(name string) ContainerVolumeFilter {
	return func(c ContainerVolume) bool { return c.Name == name }
}

// find container volumes from the db for the given filters
func FindContainerVolumes(db AgentDatabase, filters []ContainerVolumeFilter) ([]ContainerVolume, error) {
	return db.FindContainerVolumes(filters)
}
