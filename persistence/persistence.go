package persistence

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/open-horizon/anax/cutil"
	"time"
)

// ephemeral as of v2.1.0
const E_AGREEMENTS = "established_agreements" // may or may not be in agreements

const DEVMODE = "devmode"

var RemoveDatabaseOnExit bool

func SetRemoveDatabaseOnExit(remove bool) {
	RemoveDatabaseOnExit = remove
}

func GetRemoveDatabaseOnExit() bool {
	return RemoveDatabaseOnExit
}

type WorkloadInfo struct {
	URL     string `json:"url,omitempty"`
	Org     string `json:"org,omitempty"`
	Version string `json:"version,omitempty"`
	Arch    string `json:"arch,omitempty"`
}

func NewWorkloadInfo(url string, org string, version string, arch string) (*WorkloadInfo, error) {
	if url == "" || org == "" || version == "" {
		return nil, errors.New("url, org and version must be non-empty")
	}

	useArch := arch
	if useArch == "" {
		useArch = cutil.ArchString()
	}

	return &WorkloadInfo{
		URL:     url,
		Org:     org,
		Version: version,
		Arch:    useArch,
	}, nil
}

// N.B. Important!! Ensure new values are handled in Update function below
// This struct is for persisting agreements
// It implements MicroserviceInstInterface.
type EstablishedAgreement struct {
	Name                         string       `json:"name"`
	DependentServices            ServiceSpecs `json:"dependent_services"`
	Archived                     bool         `json:"archived"`
	CurrentAgreementId           string       `json:"current_agreement_id"`
	ConsumerId                   string       `json:"consumer_id"`
	CounterPartyAddress          string       `json:"counterparty_address"`
	AgreementCreationTime        uint64       `json:"agreement_creation_time"`
	AgreementAcceptedTime        uint64       `json:"agreement_accepted_time"`
	AgreementBCUpdateAckTime     uint64       `json:"agreement_bc_update_ack_time"` // V2 protocol - time when consumer acks our blockchain update
	AgreementFinalizedTime       uint64       `json:"agreement_finalized_time"`
	AgreementTerminatedTime      uint64       `json:"agreement_terminated_time"`
	AgreementForceTerminatedTime uint64       `json:"agreement_force_terminated_time"`
	AgreementExecutionStartTime  uint64       `json:"agreement_execution_start_time"`
	AgreementDataReceivedTime    uint64       `json:"agreement_data_received_time"`
	// One of the following 2 fields are set when the worker that owns deployment for this agreement, starts deploying the services in the agreement.
	CurrentDeployment               map[string]ServiceConfig `json:"current_deployment"`  // Native Horizon deployment config goes here, mutually exclusive with the extended deployment field. This field is set before the imagefetch worker starts the workload.
	ExtendedDeployment              map[string]interface{}   `json:"extended_deployment"` // All non-native deployment configs go here.
	Proposal                        string                   `json:"proposal"`
	ProposalSig                     string                   `json:"proposal_sig"`           // the proposal currently in effect
	AgreementProtocol               string                   `json:"agreement_protocol"`     // the agreement protocol being used. It is also in the proposal.
	ProtocolVersion                 int                      `json:"protocol_version"`       // the agreement protocol version being used.
	TerminatedReason                uint64                   `json:"terminated_reason"`      // the reason that the agreement was terminated
	TerminatedDescription           string                   `json:"terminated_description"` // a string form of the reason that the agreement was terminated
	AgreementProtocolTerminatedTime uint64                   `json:"agreement_protocol_terminated_time"`
	WorkloadTerminatedTime          uint64                   `json:"workload_terminated_time"`
	MeteringNotificationMsg         MeteringNotification     `json:"metering_notification,omitempty"` // the most recent metering notification received
	BlockchainType                  string                   `json:"blockchain_type,omitempty"`       // the name of the type of the blockchain
	BlockchainName                  string                   `json:"blockchain_name,omitempty"`       // the name of the blockchain instance
	BlockchainOrg                   string                   `json:"blockchain_org,omitempty"`        // the org of the blockchain instance
	RunningWorkload                 WorkloadInfo             `json:"workload_to_run,omitempty"`       // For display purposes, a copy of the workload info that this agreement is managing. It should be the same info that is buried inside the proposal.
	AgreementTimeout                uint64                   `json:"agreement_timeout"`
	ServiceDefId                    string                   `json:"service_definition_id"` // stores the microservice definiton id
}

func (c EstablishedAgreement) String() string {

	return fmt.Sprintf("Name: %v, "+
		"DependentServices: %v, "+
		"Archived: %v, "+
		"CurrentAgreementId: %v, "+
		"ConsumerId: %v, "+
		"CounterPartyAddress: %v, "+
		"CurrentDeployment (service names): %v, "+
		"ExtendedDeployment: %v, "+
		"Proposal Signature: %v, "+
		"AgreementCreationTime: %v, "+
		"AgreementExecutionStartTime: %v, "+
		"AgreementAcceptedTime: %v, "+
		"AgreementBCUpdateAckTime: %v, "+
		"AgreementFinalizedTime: %v, "+
		"AgreementDataReceivedTime: %v, "+
		"AgreementTerminatedTime: %v, "+
		"AgreementForceTerminatedTime: %v, "+
		"TerminatedReason: %v, "+
		"TerminatedDescription: %v, "+
		"Agreement Protocol: %v, "+
		"Agreement ProtocolVersion: %v, "+
		"AgreementProtocolTerminatedTime : %v, "+
		"WorkloadTerminatedTime: %v, "+
		"MeteringNotificationMsg: %v, "+
		"BlockchainType: %v, "+
		"BlockchainName: %v, "+
		"BlockchainOrg: %v, "+
		"RunningWorkload: %v, "+
		"AgreementTimeout: %v, "+
		"ServiceDefId: %v",
		c.Name, c.DependentServices, c.Archived, c.CurrentAgreementId, c.ConsumerId, c.CounterPartyAddress, ServiceConfigNames(&c.CurrentDeployment),
		"********", c.ProposalSig,
		c.AgreementCreationTime, c.AgreementExecutionStartTime, c.AgreementAcceptedTime, c.AgreementBCUpdateAckTime, c.AgreementFinalizedTime,
		c.AgreementDataReceivedTime, c.AgreementTerminatedTime, c.AgreementForceTerminatedTime, c.TerminatedReason, c.TerminatedDescription,
		c.AgreementProtocol, c.ProtocolVersion, c.AgreementProtocolTerminatedTime, c.WorkloadTerminatedTime,
		c.MeteringNotificationMsg, c.BlockchainType, c.BlockchainName, c.BlockchainOrg, c.RunningWorkload, c.AgreementTimeout, c.ServiceDefId)

}

func NewEstablishedAgreement(db AgentDatabase, name string, agreementId string, consumerId string, proposal string, protocol string, protocolVersion int, dependentSvcs ServiceSpecs, signature string, address string, bcType string, bcName string, bcOrg string, wi *WorkloadInfo, agreementTimeout uint64) (*EstablishedAgreement, error) {
	return db.NewEstablishedAgreement(name, agreementId, consumerId, proposal, protocol, protocolVersion, dependentSvcs, signature, address, bcType, bcName, bcOrg, wi,agreementTimeout)
}

func (c *EstablishedAgreement) ShortString() string {
	return fmt.Sprintf("Name: %v, "+
		"DependentServices: %v, "+
		"Archived: %v, "+
		"CurrentAgreementId: %v, "+
		"ConsumerId: %v, "+
		"CounterPartyAddress: %v, "+
		"CurrentDeployment (service names): %v, "+
		"ExtendedDeployment: %v, "+
		"AgreementCreationTime: %v, "+
		"AgreementExecutionStartTime: %v, "+
		"AgreementAcceptedTime: %v, "+
		"AgreementFinalizedTime: %v, "+
		"AgreementDataReceivedTime: %v, "+
		"AgreementTerminatedTime: %v, "+
		"AgreementForceTerminatedTime: %v, "+
		"TerminatedReason: %v, "+
		"TerminatedDescription: %v, "+
		"WorkloadTerminatedTime: %v, "+
		"RunningWorkload: %v"+
		"AgreementTimeout: %v, "+
		"ServiceDefId: %v",
		c.Name, c.DependentServices, c.Archived, c.CurrentAgreementId, c.ConsumerId, c.CounterPartyAddress,
		ServiceConfigNames(&c.CurrentDeployment), "********",
		c.AgreementCreationTime, c.AgreementExecutionStartTime, c.AgreementAcceptedTime, c.AgreementFinalizedTime,
		c.AgreementDataReceivedTime,
		c.AgreementTerminatedTime, c.AgreementForceTerminatedTime, c.TerminatedReason, c.TerminatedDescription,
		c.WorkloadTerminatedTime, c.RunningWorkload, c.AgreementTimeout, c.ServiceDefId)
}

func (a *EstablishedAgreement) GetOrg() string {
	return a.RunningWorkload.Org
}

func (a *EstablishedAgreement) GetURL() string {
	return a.RunningWorkload.URL
}

func (a *EstablishedAgreement) GetVersion() string {
	return a.RunningWorkload.Version
}

func (a *EstablishedAgreement) GetArch() string {
	return a.RunningWorkload.Arch
}

func (a *EstablishedAgreement) GetInstanceId() string {
	return a.CurrentAgreementId
}

// create a unique name
// If SpecRef is https://bluehorizon.network/microservices/network, Org is myorg, version is 2.3.1 and the instance id is "abcd1234"
// the output string will be "myorg_bluehorizon.network-microservices-network_2.3.1_abcd1234"
func (a *EstablishedAgreement) GetKey() string {
	return a.CurrentAgreementId
}

func (a *EstablishedAgreement) GetServiceDefId() string {
	return a.ServiceDefId
}

func (a *EstablishedAgreement) IsArchived() bool {
	return a.Archived
}

func (a *EstablishedAgreement) IsTopLevelService() bool {
	return true
}

func (a *EstablishedAgreement) IsAgreementLess() bool {
	return false
}

func (a *EstablishedAgreement) GetEnvVars() map[string]string {
	return map[string]string{}
}

func (a *EstablishedAgreement) GetAssociatedAgreements() []string {
	return []string{a.CurrentAgreementId}
}

func (a *EstablishedAgreement) GetParentPath() [][]ServiceInstancePathElement {
	sipe := NewServiceInstancePathElement(a.RunningWorkload.URL, a.RunningWorkload.Org, a.RunningWorkload.Version)
	return [][]ServiceInstancePathElement{[]ServiceInstancePathElement{*sipe}}
}

func (a *EstablishedAgreement) GetInstanceCreationTime() uint64 {
	return a.AgreementCreationTime
}

func (a *EstablishedAgreement) GetExecutionStartTime() uint64 {
	return a.AgreementExecutionStartTime
}

func (a *EstablishedAgreement) GetExecutionFailureCode() uint {
	return uint(a.TerminatedReason)
}

func (a *EstablishedAgreement) GetExecutionFailureDesc() string {
	return a.TerminatedDescription
}

func (a *EstablishedAgreement) GetCleanupStartTime() uint64 {
	return a.AgreementTerminatedTime
}

func (a *EstablishedAgreement) GetMaxRetries() uint {
	return 0
}

func (a *EstablishedAgreement) GetMaxRetryDuration() uint {
	return 0
}

func (a *EstablishedAgreement) GetCurrentRetryCount() uint {
	return 0
}

func (a *EstablishedAgreement) GetRetryStartTime() uint64 {
	return 0
}

func (a *EstablishedAgreement) Archive(db AgentDatabase) error {
	_, err := ArchiveEstablishedAgreement(db, a.CurrentAgreementId, a.AgreementProtocol)
	return err
}

// Return either the CurrentDeployment or the ExtendedDeployment, depending on which is set, in a form that
// implements the DeploymentConfig interface.
func (a *EstablishedAgreement) GetDeploymentConfig() DeploymentConfig {

	// If the native deployment config is in use, then create that object and return it.
	if len(a.CurrentDeployment) > 0 {
		nd := new(NativeDeploymentConfig)
		nd.Services = a.CurrentDeployment
		return nd

		// The extended deployment config must be in use, so return it. It could be kube or helm.
	} else if IsKube(a.ExtendedDeployment) {
		cd := new(KubeDeploymentConfig)
		if err := cd.FromPersistentForm(a.ExtendedDeployment); err != nil {
			glog.Errorf("Unable to convert kube deployment %v to persistent form, error %v", a.ExtendedDeployment, err)
		}
		return cd
	} else if IsHelm(a.ExtendedDeployment) {
		hd := new(HelmDeploymentConfig)
		if err := hd.FromPersistentForm(a.ExtendedDeployment); err != nil {
			glog.Errorf("Unable to convert helm deployment %v to persistent form, error %v", a.ExtendedDeployment, err)
		}
		return hd
	}

	return nil
}

func ArchiveEstablishedAgreement(db AgentDatabase, agreementId string, protocol string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, agreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.Archived = true
		c.CurrentDeployment = map[string]ServiceConfig{}
		return &c
	})
}

// set agreement state to execution started
func AgreementStateExecutionStarted(db AgentDatabase, dbAgreementId string, protocol string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.AgreementExecutionStartTime = uint64(time.Now().Unix())
		return &c
	})
}

// set agreement state to accepted, a positive reply is being sent
func AgreementStateAccepted(db AgentDatabase, dbAgreementId string, protocol string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.AgreementAcceptedTime = uint64(time.Now().Unix())
		return &c
	})
}

// set the eth signature of the proposal
func AgreementStateProposalSigned(db AgentDatabase, dbAgreementId string, protocol string, sig string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.ProposalSig = sig
		return &c
	})
}

// set the eth counterparty address when it is received from the consumer
func AgreementStateBCDataReceived(db AgentDatabase, dbAgreementId string, protocol string, address string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.CounterPartyAddress = address
		return &c
	})
}

// set the time when out agreement blockchain update message was Ack'd.
func AgreementStateBCUpdateAcked(db AgentDatabase, dbAgreementId string, protocol string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.AgreementBCUpdateAckTime = uint64(time.Now().Unix())
		return &c
	})
}

// set agreement state to finalized
func AgreementStateFinalized(db AgentDatabase, dbAgreementId string, protocol string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.AgreementFinalizedTime = uint64(time.Now().Unix())
		return &c
	})
}

// set deployment config because execution is about to begin
func AgreementDeploymentStarted(db AgentDatabase, dbAgreementId string, protocol string, deployment DeploymentConfig) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		if pf, err := deployment.ToPersistentForm(); err != nil {
			glog.Errorf("Unable to persist deployment config: (%T) %v", deployment, deployment)
		} else if deployment.IsNative() {
			for k, v := range pf {
				c.CurrentDeployment[k] = v.(ServiceConfig)
			}
		} else {
			c.ExtendedDeployment = pf
		}
		return &c
	})
}

// set agreement state to terminated
func AgreementStateTerminated(db AgentDatabase, dbAgreementId string, reason uint64, reasonString string, protocol string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.AgreementTerminatedTime = uint64(time.Now().Unix())
		c.TerminatedReason = reason
		c.TerminatedDescription = reasonString
		return &c
	})
}

// reset agreement state to not-terminated so that we can retry the termination
func AgreementStateForceTerminated(db AgentDatabase, dbAgreementId string, protocol string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.AgreementForceTerminatedTime = uint64(time.Now().Unix())
		return &c
	})
}

// set agreement state to data received
func AgreementStateDataReceived(db AgentDatabase, dbAgreementId string, protocol string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.AgreementDataReceivedTime = uint64(time.Now().Unix())
		return &c
	})
}

// set agreement state to agreement protocol terminated
func AgreementStateAgreementProtocolTerminated(db AgentDatabase, dbAgreementId string, protocol string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.AgreementProtocolTerminatedTime = uint64(time.Now().Unix())
		return &c
	})
}

// set agreement state to workload terminated
func AgreementStateWorkloadTerminated(db AgentDatabase, dbAgreementId string, protocol string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.WorkloadTerminatedTime = uint64(time.Now().Unix())
		return &c
	})
}

// set agreement state to workload terminated
func MeteringNotificationReceived(db AgentDatabase, dbAgreementId string, mn MeteringNotification, protocol string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.MeteringNotificationMsg = mn
		return &c
	})
}

func SetAgreementTimeout(db AgentDatabase, dbAgreementId string, protocol string, agTimeoutS uint64) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.AgreementTimeout = agTimeoutS
		return &c
	})
}

func SetAgreementServiceDefId(db AgentDatabase, dbAgreementId string, protocol string, svcDefId string) (*EstablishedAgreement, error) {
	return agreementStateUpdate(db, dbAgreementId, protocol, func(c EstablishedAgreement) *EstablishedAgreement {
		c.ServiceDefId = svcDefId
		return &c
	})
}

func DeleteEstablishedAgreement(db AgentDatabase, agreementId string, protocol string) error {
	return db.DeleteEstablishedAgreement(agreementId, protocol)
}

func agreementStateUpdate(db AgentDatabase, dbAgreementId string, protocol string, fn func(EstablishedAgreement) *EstablishedAgreement) (*EstablishedAgreement, error) {
	filters := make([]EAFilter, 0)
	filters = append(filters, UnarchivedEAFilter())
	filters = append(filters, IdEAFilter(dbAgreementId))

	if agreements, err := FindEstablishedAgreements(db, protocol, filters); err != nil {
		return nil, err
	} else if len(agreements) > 1 {
		return nil, fmt.Errorf("Expected only one record for dbAgreementId: %v, but retrieved: %v", dbAgreementId, agreements)
	} else if len(agreements) == 0 {
		return nil, fmt.Errorf("No record with id: %v", dbAgreementId)
	} else {
		// run this single contract through provided update function and persist it
		updated := fn(agreements[0])
		return updated, persistUpdatedAgreement(db, dbAgreementId, protocol, updated)
	}
}

// does whole-member replacements of values that are legal to change during the course of a contract's life
func persistUpdatedAgreement(db AgentDatabase, dbAgreementId string, protocol string, update *EstablishedAgreement) error {
	return db.PersistUpdatedAgreement(dbAgreementId, protocol, update)
}

func UnarchivedEAFilter() EAFilter {
	return func(e EstablishedAgreement) bool { return !e.Archived }
}

func IdEAFilter(id string) EAFilter {
	return func(e EstablishedAgreement) bool { return e.CurrentAgreementId == id }
}

func ServiceDefEAFilter(svcDefId string) EAFilter {
	return func(e EstablishedAgreement) bool { return e.ServiceDefId == svcDefId }
}

// filter on EstablishedAgreements
type EAFilter func(EstablishedAgreement) bool

// This structure is used to get the SensorUrl from the old EstablishedAgreement structure
type SensorUrls struct {
	SensorUrl []string `json:"sensor_url"`
}

func FindEstablishedAgreements(db AgentDatabase, protocol string, filters []EAFilter) ([]EstablishedAgreement, error) {
	return db.FindEstablishedAgreements(protocol, filters)
}

func FindEstablishedAgreementsAllProtocols(db AgentDatabase, protocols []string, filters []EAFilter) ([]EstablishedAgreement, error) {
	agreements := make([]EstablishedAgreement, 0)
	for _, protocol := range protocols {
		if ags, err := FindEstablishedAgreements(db, protocol, filters); err != nil {
			return nil, err
		} else {
			agreements = append(agreements, ags...)
		}
	}
	return agreements, nil
}

// =================================================================================================
// This is the persisted version of a Metering Notification. The persistence module has its own
// type for this object to avoid a circular dependency in go that would be created if this module
// tried to import the MeteringNotification type from the metering module.
//

type MeteringNotification struct {
	Amount                 uint64 `json:"amount"`                       // The number of tokens granted by this notification, rounded to the nearest minute
	StartTime              uint64 `json:"start_time"`                   // The time when the agreement started, in seconds since 1970.
	CurrentTime            uint64 `json:"current_time"`                 // The time when the notification was sent, in seconds since 1970.
	MissedTime             uint64 `json:"missed_time"`                  // The amount of time in seconds that the consumer detected missing data
	ConsumerMeterSignature string `json:"consumer_meter_signature"`     // The consumer's signature of the meter (amount, current time, agreement Id)
	AgreementHash          string `json:"agreement_hash"`               // The 32 byte SHA3 FIPS 202 hash of the proposal for the agreement.
	ConsumerSignature      string `json:"consumer_agreement_signature"` // The consumer's signature of the agreement hash.
	ConsumerAddress        string `json:"consumer_address"`             // The consumer's blockchain account/address.
	ProducerSignature      string `json:"producer_agreement_signature"` // The producer's signature of the agreement
	BlockchainType         string `json:"blockchain_type"`              // The type of the blockchain that this notification is intended to work with
}

func (m MeteringNotification) String() string {
	return fmt.Sprintf("Amount: %v, "+
		"StartTime: %v, "+
		"CurrentTime: %v, "+
		"Missed Time: %v, "+
		"ConsumerMeterSignature: %v, "+
		"AgreementHash: %v, "+
		"ConsumerSignature: %v, "+
		"ConsumerAddress: %v, "+
		"ProducerSignature: %v, "+
		"BlockchainType: %v",
		m.Amount, m.StartTime, m.CurrentTime, m.MissedTime, m.ConsumerMeterSignature,
		m.AgreementHash, m.ConsumerSignature, m.ConsumerAddress, m.ProducerSignature,
		m.BlockchainType)
}
