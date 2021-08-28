package bolt

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	bolt "go.etcd.io/bbolt"
)

func (db *AgentBoltDB) FindEstablishedAgreements(protocol string, filters []EAFilter) ([]EstablishedAgreement, error) {
	agreements := make([]EstablishedAgreement, 0)

	// fetch contracts
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(E_AGREEMENTS + "-" + protocol)); b != nil {
			b.ForEach(func(k, v []byte) error {

				var e EstablishedAgreement

				if err := json.Unmarshal(v, &e); err != nil {
					glog.Errorf("Unable to deserialize db record to EstablishedAgreement: %v", v)
				} else {
					// this might be agreement from the old EstablishedAgreement structure where SensorUrl was used.
					// will convert it to new using DependentServices
					if e.DependentServices == nil {
						var sensor_urls SensorUrls
						if err := json.Unmarshal(v, &sensor_urls); err != nil {
							glog.Errorf("Unable to deserialize db record to SensorUrl: %v", v)
						} else {
							e.DependentServices = []ServiceSpec{}
							if sensor_urls.SensorUrl != nil && len(sensor_urls.SensorUrl) > 0 {
								for _, url := range sensor_urls.SensorUrl {
									e.DependentServices = append(e.DependentServices, ServiceSpec{Url: url})
								}
							}
						}
					}

					if !e.Archived {
						glog.V(5).Infof("Demarshalled agreement in DB: %v", e)
					}
					exclude := false
					for _, filterFn := range filters {
						if !filterFn(e) {
							exclude = true
						}
					}
					if !exclude {
						agreements = append(agreements, e)
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
		return agreements, nil
	}	
}

func (db *AgentBoltDB) NewEstablishedAgreement(name string, agreementId string, consumerId string, proposal string, protocol string, protocolVersion int, dependentSvcs ServiceSpecs, signature string, address string, bcType string, bcName string, bcOrg string, wi *WorkloadInfo, agreementTimeout uint64) (*EstablishedAgreement, error) {

	if name == "" || agreementId == "" || consumerId == "" || proposal == "" || protocol == "" || protocolVersion == 0 {
		return nil, errors.New("Agreement id, consumer id, proposal, protocol, or protocol version are empty, cannot persist")
	}

	var filters []EAFilter
	filters = append(filters, UnarchivedEAFilter())
	filters = append(filters, IdEAFilter(agreementId))

	if agreements, err := FindEstablishedAgreements(protocol, filters); err != nil {
		return nil, err
	} else if len(agreements) != 0 {
		return nil, fmt.Errorf("Not expecting any records with id: %v, found %v", agreementId, agreements)
	}

	newAg := &EstablishedAgreement{
		Name:                            name,
		DependentServices:               dependentSvcs,
		Archived:                        false,
		CurrentAgreementId:              agreementId,
		ConsumerId:                      consumerId,
		CounterPartyAddress:             address,
		AgreementCreationTime:           uint64(time.Now().Unix()),
		AgreementAcceptedTime:           0,
		AgreementBCUpdateAckTime:        0,
		AgreementFinalizedTime:          0,
		AgreementTerminatedTime:         0,
		AgreementForceTerminatedTime:    0,
		AgreementExecutionStartTime:     0,
		AgreementDataReceivedTime:       0,
		CurrentDeployment:               map[string]ServiceConfig{},
		ExtendedDeployment:              map[string]interface{}{},
		Proposal:                        proposal,
		ProposalSig:                     signature,
		AgreementProtocol:               protocol,
		ProtocolVersion:                 protocolVersion,
		TerminatedReason:                0,
		TerminatedDescription:           "",
		AgreementProtocolTerminatedTime: 0,
		WorkloadTerminatedTime:          0,
		MeteringNotificationMsg:         MeteringNotification{},
		BlockchainType:                  bcType,
		BlockchainName:                  bcName,
		BlockchainOrg:                   bcOrg,
		RunningWorkload:                 *wi,
		AgreementTimeout:                agreementTimeout,
	}

	return newAg, db.db.Update(func(tx *bolt.Tx) error {

		if b, err := tx.CreateBucketIfNotExists([]byte(E_AGREEMENTS + "-" + protocol)); err != nil {
			return err
		} else if bytes, err := json.Marshal(newAg); err != nil {
			return fmt.Errorf("Unable to marshal new record: %v", err)
		} else if err := b.Put([]byte(agreementId), []byte(bytes)); err != nil {
			return fmt.Errorf("Unable to persist agreement: %v", err)
		}

		// success, close tx
		return nil
	})	
}

func (db *AgentBoltDB) DeleteEstablishedAgreement(agreementId string, protocol string) error {

	if agreementId == "" {
		return errors.New("Agreement id empty, cannot remove")
	} else {

		filters := make([]EAFilter, 0)
		filters = append(filters, UnarchivedEAFilter())
		filters = append(filters, IdEAFilter(agreementId))

		if agreements, err := FindEstablishedAgreements(protocol, filters); err != nil {
			return err
		} else if len(agreements) != 1 {
			return fmt.Errorf("Expecting 1 records with id: %v, found %v", agreementId, agreements)
		} else {

			return db.db.Update(func(tx *bolt.Tx) error {

				if b, err := tx.CreateBucketIfNotExists([]byte(E_AGREEMENTS + "-" + protocol)); err != nil {
					return err
				} else if err := b.Delete([]byte(agreementId)); err != nil {
					return fmt.Errorf("Unable to delete agreement: %v", err)
				} else {
					return nil
				}
			})
		}
	}
}

// does whole-member replacements of values that are legal to change during the course of a contract's life
func (db *AgentBoltDB) PersistUpdatedAgreement(dbAgreementId string, protocol string, update *EstablishedAgreement) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		if b, err := tx.CreateBucketIfNotExists([]byte(E_AGREEMENTS + "-" + protocol)); err != nil {
			return err
		} else {
			current := b.Get([]byte(dbAgreementId))
			var mod EstablishedAgreement

			if current == nil {
				return fmt.Errorf("No agreement with given id available to update: %v", dbAgreementId)
			} else if err := json.Unmarshal(current, &mod); err != nil {
				return fmt.Errorf("Failed to unmarshal agreement DB data: %v. Error: %v", string(current), err)
			} else {

				// This code is running in a database transaction. Within the tx, the current record is
				// read and then updated according to the updates within the input update record. It is critical
				// to check for correct data transitions within the tx.
				if !mod.Archived { // 1 transition from false to true
					mod.Archived = update.Archived
				}
				if len(mod.CounterPartyAddress) == 0 { // 1 transition from empty to non-empty
					mod.CounterPartyAddress = update.CounterPartyAddress
				}
				if mod.AgreementAcceptedTime == 0 { // 1 transition from zero to non-zero
					mod.AgreementAcceptedTime = update.AgreementAcceptedTime
				}
				if mod.AgreementBCUpdateAckTime == 0 { // 1 transition from zero to non-zero
					mod.AgreementBCUpdateAckTime = update.AgreementBCUpdateAckTime
				}
				if mod.AgreementFinalizedTime == 0 { // 1 transition from zero to non-zero
					mod.AgreementFinalizedTime = update.AgreementFinalizedTime
				}
				if mod.AgreementTerminatedTime == 0 { // 1 transition from zero to non-zero
					mod.AgreementTerminatedTime = update.AgreementTerminatedTime
				}
				if mod.AgreementForceTerminatedTime < update.AgreementForceTerminatedTime { // always moves forward
					mod.AgreementForceTerminatedTime = update.AgreementForceTerminatedTime
				}
				if mod.AgreementExecutionStartTime == 0 { // 1 transition from zero to non-zero
					mod.AgreementExecutionStartTime = update.AgreementExecutionStartTime
				}
				if mod.AgreementDataReceivedTime < update.AgreementDataReceivedTime { // always moves forward
					mod.AgreementDataReceivedTime = update.AgreementDataReceivedTime
				}
				// valid transitions are from empty to non-empty to empty, ad infinitum
				if (len(mod.CurrentDeployment) == 0 && len(update.CurrentDeployment) != 0) || (len(mod.CurrentDeployment) != 0 && len(update.CurrentDeployment) == 0) {
					mod.CurrentDeployment = update.CurrentDeployment
				}
				if len(mod.ExtendedDeployment) == 0 && len(update.ExtendedDeployment) != 0 { // valid transitions are from empty to non-empty
					mod.ExtendedDeployment = update.ExtendedDeployment
				}
				if mod.TerminatedReason == 0 { // 1 transition from zero to non-zero
					mod.TerminatedReason = update.TerminatedReason
				}
				if mod.TerminatedDescription == "" { // 1 transition from empty to non-empty
					mod.TerminatedDescription = update.TerminatedDescription
				}
				if mod.AgreementProtocolTerminatedTime == 0 { // 1 transition from zero to non-zero
					mod.AgreementProtocolTerminatedTime = update.AgreementProtocolTerminatedTime
				}
				if mod.WorkloadTerminatedTime == 0 { // 1 transition from zero to non-zero
					mod.WorkloadTerminatedTime = update.WorkloadTerminatedTime
				}
				if update.MeteringNotificationMsg != (MeteringNotification{}) { // only save non-empty values
					mod.MeteringNotificationMsg = update.MeteringNotificationMsg
				}
				if mod.BlockchainType == "" { // 1 transition from empty to non-empty
					mod.BlockchainType = update.BlockchainType
				}
				if mod.BlockchainName == "" { // 1 transition from empty to non-empty
					mod.BlockchainName = update.BlockchainName
				}
				if mod.BlockchainOrg == "" { // 1 transition from empty to non-empty
					mod.BlockchainOrg = update.BlockchainOrg
				}
				if mod.ProposalSig == "" { // 1 transition from empty to non-empty
					mod.ProposalSig = update.ProposalSig
				}
				if mod.ServiceDefId == "" { // transition add microservice definition id
					mod.ServiceDefId = update.ServiceDefId
				}

				if serialized, err := json.Marshal(mod); err != nil {
					return fmt.Errorf("Failed to serialize contract record: %v. Error: %v", mod, err)
				} else if err := b.Put([]byte(dbAgreementId), serialized); err != nil {
					return fmt.Errorf("Failed to write contract record with key: %v. Error: %v", dbAgreementId, err)
				} else {
					glog.V(2).Infof("Succeeded updating agreement id record to %v", mod)
					return nil
				}
			}
		}
	})	
}