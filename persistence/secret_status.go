package persistence

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
)

const SECRET_STATUS = "secret_status"

type MicroserviceSecretStatusInst struct {
	MsInstKey     string
	ESSToken      string
	SecretsStatus map[string]*SecretStatus
}

type SecretStatus struct {
	SecretName string `json:"secret_name"`
	UpdateTime uint64 `json:"update_time"`
}

// for log and testing
func (w MicroserviceSecretStatusInst) String() string {
	return fmt.Sprintf("MsInstKey: %v, "+
		"ESSToken: ********, "+
		"SecretsStatus: %v",
		w.MsInstKey, w.SecretsStatus)
}

func (s SecretStatus) String() string {
	return fmt.Sprintf("{SecretName: %v, "+
		"UpdateTime: %v}",
		s.SecretName, s.UpdateTime)
}

func NewMSSInst(db AgentDatabase, msInstKey string, essToken string) (*MicroserviceSecretStatusInst, error) {
	if msInstKey == "" || essToken == "" {
		return nil, errors.New("microserviceInstanceKey or essToken is empty, cannot persist")
	}

	new_secret_status_inst := &MicroserviceSecretStatusInst{
		MsInstKey:     msInstKey,
		ESSToken:      essToken,
		SecretsStatus: make(map[string]*SecretStatus),
	}
	return saveMSSInst(db, new_secret_status_inst)
}

func NewSecretStatus(secretName string, updateTime uint64) *SecretStatus {
	return &SecretStatus{
		SecretName: secretName,
		UpdateTime: updateTime,
	}
}

func (w MicroserviceSecretStatusInst) GetKey() string {
	return w.MsInstKey
}

// save the given microserviceSecretStatus instance into the db. Key: MsInstKey, Value: MicroserviceSecretStatus Object
func saveMSSInst(db AgentDatabase, new_secret_status_inst *MicroserviceSecretStatusInst) (*MicroserviceSecretStatusInst, error) {
	return db.SaveMSSInst(new_secret_status_inst)
}

func FindMSSInstWithKey(db AgentDatabase, ms_inst_key string) (*MicroserviceSecretStatusInst, error) {
	return db.FindMSSInstWithKey(ms_inst_key)
}

func FindMSSInstWithESSToken(db AgentDatabase, ess_token string) (*MicroserviceSecretStatusInst, error) {
	return db.FindMSSInstWithESSToken(ess_token)
}

// delete a microserviceSecretStatus instance from db. It will NOT return error if it does not exist in the db
func DeleteMSSInstWithKey(db AgentDatabase, ms_inst_key string) (*MicroserviceSecretStatusInst, error) {
	return db.DeleteMSSInstWithKey(ms_inst_key)
}

func DeleteMSSInstWithESSToken(db AgentDatabase, ess_token string) (*MicroserviceSecretStatusInst, error) {
	return db.DeleteMSSInstWithESSToken(ess_token)
}

func SaveSecretStatus(db AgentDatabase, ms_inst_key string, secret_status *SecretStatus) (*MicroserviceSecretStatusInst, error) {
	return mssInstStateUpdate(db, ms_inst_key, func(c MicroserviceSecretStatusInst) *MicroserviceSecretStatusInst {
		c.SecretsStatus[secret_status.SecretName] = secret_status
		return &c
	})
}

func FindSecretStatus(db AgentDatabase, ms_inst_key string, secret_name string) (*SecretStatus, error) {
	secStatus := &SecretStatus{}
	mssinst, err := FindMSSInstWithKey(db, ms_inst_key)
	if err != nil {
		return secStatus, err
	}

	return mssinst.SecretsStatus[secret_name], nil
}

func FindUpdatedSecretsForMSSInstance(db AgentDatabase, ms_inst_key string) ([]string, error) {
	updatedSecretNames := make([]string, 0)
	if mssInst, err := FindMSSInstWithKey(db, ms_inst_key); err != nil {
		return updatedSecretNames, err
	} else {
		// get secretsStatus map for this instance
		secretsStatus := mssInst.SecretsStatus
		// Find secrets for given mssInst Key  in "Secrets" bucket
		secrets, err := FindAllSecretsForMS(db, mssInst.GetKey()) // list of secrets retrieved from "Secret" bucket
		if err != nil {
			return updatedSecretNames, err
		} else if secrets == nil || len(secrets.SecretsMap) == 0 {
			return updatedSecretNames, nil
		}

		// Go through the "Secrets" bucket secrets
		for secName, secret := range secrets.SecretsMap {
			// 1. If secret TimeLastUpdated == TimeCreated, no update
			// 2. If MSInstance secretStatus has no record, and update time > create time, has been updated
			// 3. If MSInstance secretStatus doesn't have this secret, and update time > create time, has been update
			// 4. If MSInstance secretStatus has this secret, and secret update time > secret status update time, has been updated

			if secret.TimeLastUpdated == 0 || secret.TimeLastUpdated == secret.TimeCreated {
				// the secret is not updated since created
				continue
			} else if secret.TimeLastUpdated > secret.TimeCreated {
				if len(secretsStatus) == 0 {
					updatedSecretNames = append(updatedSecretNames, secName)
				} else if secStat, ok := secretsStatus[secName]; !ok {
					updatedSecretNames = append(updatedSecretNames, secName)
				} else if secret.TimeLastUpdated > secStat.UpdateTime {
					updatedSecretNames = append(updatedSecretNames, secName)
				}
			}
		}
	}
	return updatedSecretNames, nil
}

// update the microserviceSecretStatus instance
func mssInstStateUpdate(db AgentDatabase, ms_inst_key string, fn func(MicroserviceSecretStatusInst) *MicroserviceSecretStatusInst) (*MicroserviceSecretStatusInst, error) {

	if mss, err := FindMSSInstWithKey(db, ms_inst_key); err != nil {
		return nil, err
	} else if mss == nil {
		return nil, fmt.Errorf("No record with key: %v", ms_inst_key)
	} else if updated := fn(*mss); updated == nil {
		// if SecretsStatus[secretName] doesn't exist, the field is not set, return nil with no error
		return nil, nil
	} else {
		// run this single contract through provided update function and persist it
		return updated, persistUpdatedMSSInst(db, ms_inst_key, updated)
	}
}

func persistUpdatedMSSInst(db AgentDatabase, ms_inst_key string, update *MicroserviceSecretStatusInst) error {
	return db.PersistUpdatedMSSInst(ms_inst_key, update)
}
