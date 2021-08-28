package persistence

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/open-horizon/anax/i18n"
	"time"
)

const NODE_SURFACEERR = "nodesurfaceerror"

// The format for node eventlog errors surfaced to the exchange
type SurfaceError struct {
	Record_id  string       `json:"record_id"`
	Message    string       `json:"message"`
	Event_code string       `json:"event_code"`
	Hidden     bool         `json:"hidden"`
	Workload   WorkloadInfo `json:"workload"`
	Timestamp  string       `json:"timestamp"`
}

// FindSurfaceErrors returns the surface errors currently in the local db
func FindSurfaceErrors(db AgentDatabase) ([]SurfaceError, error) {
	return db.FindSurfaceErrors()
}

// SaveSurfaceErrors saves the provided list of surface errors to the local db
func SaveSurfaceErrors(db AgentDatabase, surfaceErrors []SurfaceError) error {
	return db.SaveSurfaceErrors(surfaceErrors)
}

// DeleteSurfaceErrors delete node surface errors from the local database
func DeleteSurfaceErrors(db AgentDatabase) error {
	return db.DeleteSurfaceErrors()
}

// NewErrorLog takes an eventLog object and puts it in the local db and exchange if it should be surfaced
func NewErrorLog(db AgentDatabase, eventLog EventLog) bool {
	if !IsSurfaceType(eventLog.EventCode) || !(eventLog.SourceType == SRC_TYPE_AG || eventLog.SourceType == SRC_TYPE_SVC) {
		return false
	}
	currentErrors, err := FindSurfaceErrors(db)
	if err != nil {
		glog.V(3).Infof("Error getting surface errors from local db. %v", err)
		return false
	}
	found := false
	for i, currentError := range currentErrors {
		if MatchWorkload(GetEventLogObject(db, nil, currentError.Record_id), GetEventLogObject(db, nil, eventLog.Id)) {
			hiddenField := currentError.Hidden
			if eventLog.EventCode != currentError.Event_code {
				hiddenField = false
			}
			currentErrors[i] = NewSurfaceError(eventLog)
			currentErrors[i].Hidden = hiddenField
			found = true
		}
	}
	if !found {
		currentErrors = append(currentErrors, NewSurfaceError(eventLog))
	}
	if err = SaveSurfaceErrors(db, currentErrors); err != nil {
		glog.V(3).Infof("Error saving surface errors to local db. %v", err)
	}
	return true
}

// getErrorTypeList returns a slice containing the error types to surface to the exchange
func getErrorTypeList() []string {
	return []string{
		EC_ERROR_IMAGE_LOADE,
		EC_ERROR_IN_DEPLOYMENT_CONFIG,
		EC_ERROR_START_CONTAINER,
		EC_CANCEL_AGREEMENT_EXECUTION_TIMEOUT,
		EC_CANCEL_AGREEMENT_SERVICE_SUSPENDED,
		EC_ERROR_SERVICE_CONFIG,
		EC_ERROR_START_SERVICE,
		EC_ERROR_START_DEPENDENT_SERVICE,
		EC_DEPENDENT_SERVICE_FAILED,
	}

}

// IsSurfaceType returns true if the string parameter is a type to surface to the exchange
func IsSurfaceType(errorType string) bool {
	for _, surfaceType := range getErrorTypeList() {
		if errorType == surfaceType {
			return true
		}
	}
	return false
}

// MatchWorkload function checks if the 2 eventlog parameters have matching workloads
func MatchWorkload(error1 EventLog, error2 EventLog) bool {
	var source1Workload WorkloadInfo
	var source2Workload WorkloadInfo
	if source1, ok := error1.Source.(AgreementEventSource); ok {
		source1Workload = WorkloadInfo{URL: source1.RunningWorkload.URL, Org: source1.RunningWorkload.Org, Arch: source1.RunningWorkload.Arch, Version: source1.RunningWorkload.Version}
	} else if source1, ok := error1.Source.(ServiceEventSource); ok {
		source1Workload = WorkloadInfo{URL: source1.ServiceUrl, Org: source1.Org, Arch: source1.Arch, Version: source1.Version}
	} else {
		return false
	}
	if source2, ok := error2.Source.(AgreementEventSource); ok {
		source2Workload = WorkloadInfo{URL: source2.RunningWorkload.URL, Org: source2.RunningWorkload.Org, Arch: source2.RunningWorkload.Arch, Version: source2.RunningWorkload.Version}
	} else if source2, ok := error2.Source.(ServiceEventSource); ok {
		source2Workload = WorkloadInfo{URL: source2.ServiceUrl, Org: source2.Org, Arch: source2.Arch, Version: source2.Version}
	} else {
		return false
	}

	return (source1Workload.URL == source2Workload.URL && source1Workload.Org == source2Workload.Org)
}

// NewSurfaceError returns a surface error from the eventlog parameter
func NewSurfaceError(eventLog EventLog) SurfaceError {
	timestamp := time.Unix((int64)(eventLog.Timestamp), 0).String()
	newErr := SurfaceError{Record_id: eventLog.Id, Message: fmt.Sprintf("%s: %v", eventLog.MessageMeta.MessageKey, eventLog.MessageMeta.MessageArgs), Event_code: eventLog.EventCode, Hidden: false, Workload: GetWorkloadInfo(eventLog), Timestamp: timestamp}
	if eventLog.MessageMeta != nil && eventLog.MessageMeta.MessageKey != "" {
		newErr.Message = i18n.GetMessagePrinter().Sprintf(eventLog.MessageMeta.MessageKey, eventLog.MessageMeta.MessageArgs...)
	}
	return newErr
}

func GetWorkloadInfo(eventLog EventLog) WorkloadInfo {
	if source, ok := eventLog.Source.(AgreementEventSource); ok {
		return WorkloadInfo{URL: source.RunningWorkload.URL, Org: source.RunningWorkload.Org, Arch: source.RunningWorkload.Arch, Version: source.RunningWorkload.Version}
	} else if source, ok := eventLog.Source.(*AgreementEventSource); ok {
		return WorkloadInfo{URL: source.RunningWorkload.URL, Org: source.RunningWorkload.Org, Arch: source.RunningWorkload.Arch, Version: source.RunningWorkload.Version}
	} else if source, ok := eventLog.Source.(ServiceEventSource); ok {
		return WorkloadInfo{URL: source.ServiceUrl, Org: source.Org, Arch: source.Arch, Version: source.Version}
	} else if source, ok := eventLog.Source.(*ServiceEventSource); ok {
		return WorkloadInfo{URL: source.ServiceUrl, Org: source.Org, Arch: source.Arch, Version: source.Version}
	}
	return WorkloadInfo{}
}
