package persistence

import (
	"github.com/open-horizon/anax/config"
	"github.com/open-horizon/anax/policy" // needed??
	"github.com/open-horizon/anax/externalpolicy"
	"golang.org/x/text/message"
)

// An agentbot can be configured to run with several different databases. Currently only bbolt is supported.
// This file contains the abstract interface representing 
// the database handle used by the runtime to access the real database.

type AgentDatabase interface {

	// Database related functions
	Initialize(cfg *config.HorizonConfig) error
	Close()

	FindAttributeByKey(attributeid string) (*Attribute, error)
	FindApplicableAttributes(serviceUrl string, org string) ([]Attribute, error)
	UpdateAttribute(attributeid string, attr *Attribute) error
	DeleteAttribute(attributeid string) (*Attribute, error)

	SaveContainerVolume(container_volume *ContainerVolume) error
	FindContainerVolumes(filters []ContainerVolumeFilter) ([]ContainerVolume, error)

	UpdateExchangeDevice(self *ExchangeDevice, deviceId string, invalidateToken bool, fn func(ExchangeDevice) *ExchangeDevice) (*ExchangeDevice, error)
	SaveNewExchangeDevice(id string, token string, name string, nodeType string, ha bool, organization string, pattern string, configstate string) (*ExchangeDevice, error)
	FindExchangeDevice() (*ExchangeDevice, error)
	DeleteExchangeDevice() error

	SaveLastUnregistrationTime(last_unreg_time uint64) error
	GetLastUnregistrationTime() (uint64, error)
	SaveEventLog(event_log *EventLog) error
	FindEventLogWithKey(key string) (*EventLog, error)
	FindEventLogs(filters []EventLogFilter) ([]EventLog, error)
	FindEventLogsWithSelectors(all_logs bool, selectors map[string][]Selector, msgPrinter *message.Printer) ([]EventLog, error)
	FindAllEventLogs() ([]EventLog, error)

	FindExchangeChangeState() (*ChangeState, error)
	SaveExchangeChangeState(changeID uint64) error
	DeleteExchangeChangeState() error

	SaveOrUpdateMicroserviceDef(msdef *MicroserviceDefinition) error
	FindMicroserviceDefWithKey(key string) (*MicroserviceDefinition, error)
	FindMicroserviceDefs(filters []MSFilter) ([]MicroserviceDefinition, error)
	PersistUpdatedMicroserviceDef(key string, update *MicroserviceDefinition) error
	FindMicroserviceInstance(url string, org string, version string, instance_id string) (*MicroserviceInstance, error)
	FindMicroserviceInstanceWithKey(key string) (*MicroserviceInstance, error)
	FindMicroserviceInstances(filters []MIFilter) ([]MicroserviceInstance, error)
	PersistUpdatedMicroserviceInstance(key string, update *MicroserviceInstance) error
	DeleteMicroserviceInstance(key string) (*MicroserviceInstance, error)
	SaveMicroserviceInstance(new_inst *MicroserviceInstance) (*MicroserviceInstance, error)

	FindSavedNodeExchPattern() (string, error)
	SaveNodeExchPattern(nodePatternName string) error
	DeleteNodeExchPattern() error

	FindNodePolicy() (*externalpolicy.ExternalPolicy, error)
	SaveNodePolicy(nodePolicy *externalpolicy.ExternalPolicy) error
	DeleteNodePolicy() error
	GetNodePolicyLastUpdated_Exch() (string, error)
	SaveNodePolicyLastUpdated_Exch(lastUpdated string) error
	DeleteNodePolicyLastUpdated_Exch() error

	FindNodeStatus() ([]WorkloadStatus, error)
	SaveNodeStatus(status []WorkloadStatus) error
	DeleteNodeStatus() error

	NewEstablishedAgreement(name string, agreementId string, consumerId string, proposal string, protocol string, protocolVersion int, dependentSvcs ServiceSpecs, signature string, address string, bcType string, bcName string, bcOrg string, wi *WorkloadInfo, agreementTimeout uint64) (*EstablishedAgreement, error)
	FindEstablishedAgreements(protocol string, filters []EAFilter) ([]EstablishedAgreement, error)
	DeleteEstablishedAgreement(agreementId string, protocol string) error
	PersistUpdatedAgreement(dbAgreementId string, protocol string, update *EstablishedAgreement) error
	NewEstablishedAgreement_Old(name string, agreementId string, consumerId string, proposal string, protocol string, protocolVersion int, sensorUrl []string, signature string, address string, bcType string, bcName string, bcOrg string, wi *WorkloadInfo) (*EstablishedAgreement_Old, error)   // used in unit test, should be here?

	SaveMSSInst(new_secret_status_inst *MicroserviceSecretStatusInst) (*MicroserviceSecretStatusInst, error)
	FindMSSInstWithKey(ms_inst_key string) (*MicroserviceSecretStatusInst, error)
	FindMSSInstWithESSToken(ess_token string) (*MicroserviceSecretStatusInst, error)
	DeleteMSSInstWithKey(ms_inst_key string) (*MicroserviceSecretStatusInst, error)
	DeleteMSSInstWithESSToken(ess_token string) (*MicroserviceSecretStatusInst, error)
	PersistUpdatedMSSInst(ms_inst_key string, update *MicroserviceSecretStatusInst) error

	SaveAgreementSecrets(agId string, secretsList *[]PersistedServiceSecret) error
	FindAgreementSecrets(agId string) (*[]PersistedServiceSecret, error)
	DeleteAgreementSecrets(agId string) error
	SaveAllSecretsForService(msInstId string, secretToSaveAll *PersistedServiceSecrets) error
	FindAllSecretsForMS(msInstId string) (*PersistedServiceSecrets, error)
	FindAllServiceSecretsWithFilters(filters []SecFilter) ([]PersistedServiceSecrets, error)
	DeleteSecrets(secName string, msInstId string) (*PersistedServiceSecret, error)

	FindSurfaceErrors() ([]SurfaceError, error)
	SaveSurfaceErrors(surfaceErrors []SurfaceError) error
	DeleteSurfaceErrors() error

	FindNodeUserInput() ([]policy.UserInput, error)
	SaveNodeUserInput(userInput []policy.UserInput) error
	DeleteNodeUserInput() error
	GetNodeUserInputHash_Exch() ([]byte, error)
	SaveNodeUserInputHash_Exch(userInputHash []byte) error
	DeleteNodeUserInputHash_Exch() error

}
