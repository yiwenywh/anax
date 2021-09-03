package persistence

const NODE_STATUS = "node_status"

type WorkloadStatus struct {
	AgreementId    string            `json:"agreementId"`
	ServiceURL     string            `json:"serviceUrl,omitempty"`
	Org            string            `json:"orgid,omitempty"`
	Version        string            `json:"version,omitempty"`
	Arch           string            `json:"arch,omitempty"`
	Containers     []ContainerStatus `json:"containerStatus"`
	OperatorStatus interface{}       `json:"operatorStatus,omitempty"`
	ConfigState    string            `json:"configState,omitempty"`
}

type ContainerStatus struct {
	Name    string `json:"name"`
	Image   string `json:"image"`
	Created int64  `json:"created"`
	State   string `json:"state"`
}

// FindNodeStatus returns the node status currently in the local db
func FindNodeStatus(db AgentDatabase) ([]WorkloadStatus, error) {
	return db.FindNodeStatus()
}

// SaveNodeStatus saves the provided node status to the local db
func SaveNodeStatus(db AgentDatabase, status []WorkloadStatus) error {
	return db.SaveNodeStatus(status)
}

// DeleteSurfaceErrors delete node status from the local database
func DeleteNodeStatus(db AgentDatabase) error {
	return db.DeleteNodeStatus()
}
