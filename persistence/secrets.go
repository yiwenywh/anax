package persistence

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/open-horizon/anax/cutil"
	"github.com/open-horizon/anax/exchangecommon"
	"github.com/open-horizon/anax/semanticversion"
	"time"
)

// Secrets associated with their service instances. Keyed by microservice instance
const SECRETS = "secrets"

// The secrets from the agreements. Keyed by agreement id
const AGREEMENT_SECRETS = "agreement_secrets"

type PersistedServiceSecret struct {
	SvcOrgid        string
	SvcUrl          string
	SvcArch         string
	SvcVersionRange string
	SvcSecretName   string
	SvcSecretValue  string
	AgreementIds    []string
	ContainerIds    []string
	TimeCreated     uint64
	TimeLastUpdated uint64
}

type PersistedServiceSecrets struct {
	MsInstKey  string
	MsInstVers string // Version in the individual secrets is the range from the secret binding. This is the specific version associated with the msdef
	MsInstUrl  string
	MsInstOrg  string
	SecretsMap map[string]*PersistedServiceSecret
}

func PersistedSecretFromPolicySecret(inputSecretBindings []exchangecommon.SecretBinding, agId string) []PersistedServiceSecret {
	outputSecrets := []PersistedServiceSecret{}
	for _, secBind := range inputSecretBindings {
		for _, secArray := range secBind.Secrets {
			for secName, secDetails := range secArray {
				outputSecrets = append(outputSecrets, PersistedServiceSecret{SvcOrgid: secBind.ServiceOrgid, SvcUrl: secBind.ServiceUrl, SvcArch: secBind.ServiceArch, SvcVersionRange: secBind.ServiceVersionRange, SvcSecretName: secName, SvcSecretValue: secDetails, AgreementIds: []string{agId}})
			}
		}
	}

	return outputSecrets
}

// Save the secret bindings from an agreement
// This bucket is used to keep the secret information until such time that the microservice instance id is created
// After that id exists, the secrets will be saved in the SECRETS bucket keyed by ms instance id
func SaveAgreementSecrets(db AgentDatabase, agId string, secretsList *[]PersistedServiceSecret) error {
	return db.SaveAgreementSecrets(agId, secretsList)
}

func FindAgreementSecrets(db AgentDatabase, agId string) (*[]PersistedServiceSecret, error) {
	return db.FindAgreementSecrets(agId)
}

func DeleteAgreementSecrets(db AgentDatabase, agId string) error {
	return db.DeleteAgreementSecrets(agId)
}

// Saves the given secret to the agent db
func SaveSecret(db AgentDatabase, secretName string, msInstKey string, msInstVers string, secretToSave *PersistedServiceSecret) error {
	if secretToSave == nil {
		return nil
	}
	secretToSaveAll := &PersistedServiceSecrets{MsInstKey: msInstKey, MsInstOrg: secretToSave.SvcOrgid, MsInstUrl: secretToSave.SvcUrl, MsInstVers: msInstVers, SecretsMap: map[string]*PersistedServiceSecret{}}
	if allSecsForServiceInDB, err := FindAllSecretsForMS(db, msInstKey); err != nil {
		return fmt.Errorf("Failed to get all secrets for microservice %v. Error was: %v", msInstKey, err)
	} else if allSecsForServiceInDB != nil {
		secretToSaveAll = allSecsForServiceInDB
	}

	// if TimeCreated == TimeLastUpdated, then secrets API won't return secret name as updated secret
	timestamp := uint64(time.Now().Unix())
	if secretToSave.TimeCreated == 0 {
		secretToSave.TimeCreated = timestamp
	}

	if mergedSec, ok := secretToSaveAll.SecretsMap[secretName]; ok {
		mergedSec.AgreementIds = cutil.MergeSlices(mergedSec.AgreementIds, secretToSave.AgreementIds)
		mergedSec.ContainerIds = cutil.MergeSlices(mergedSec.ContainerIds, secretToSave.ContainerIds)
		if mergedSec.SvcSecretValue != secretToSave.SvcSecretValue {
			mergedSec.TimeLastUpdated = timestamp
			mergedSec.SvcSecretValue = secretToSave.SvcSecretValue
		}
		secretToSaveAll.SecretsMap[secretName] = mergedSec
	} else {
		secretToSave.TimeLastUpdated = uint64(time.Now().Unix())
		secretToSaveAll.SecretsMap[secretName] = secretToSave
	}

	return SaveAllSecretsForService(db, msInstKey, secretToSaveAll)
}

func SaveAllSecretsForService(db AgentDatabase, msInstId string, secretToSaveAll *PersistedServiceSecrets) error {
	return db.SaveAllSecretsForService(msInstId, secretToSaveAll)
}

// Gets the secret from the database, no error returned if none is found in the db
func FindAllSecretsForMS(db AgentDatabase, msInstId string) (*PersistedServiceSecrets, error) {
	return db.FindAllSecretsForMS(msInstId)
}

// Find a particular secret, if a version range is provided it must match the exact range on the secret, if a specific version is given return the first matching secret range
func FindSingleSecretForService(db AgentDatabase, secName string, msInstId string) (*PersistedServiceSecret, error) {
	allSec, err := FindAllSecretsForMS(db, msInstId)
	if err != nil {
		return nil, err
	}

	if allSec != nil {
		retSec := allSec.SecretsMap[secName]
		return retSec, nil
	}

	return nil, nil
}

func AddContainerIdToSecret(db AgentDatabase, secName string, msInstId string, msDefVers string, containerId string) error {
	sec, err := FindSingleSecretForService(db, secName, msInstId)
	if err != nil {
		return err
	}

	if !cutil.SliceContains(sec.ContainerIds, containerId) {
		sec.ContainerIds = append(sec.ContainerIds, containerId)
		err = SaveSecret(db, secName, msInstId, msDefVers, sec)
		if err != nil {
			return err
		}
	}
	return nil
}

type SecFilter func(PersistedServiceSecrets) bool

func UrlSecFilter(serviceUrl string) SecFilter {
	return func(e PersistedServiceSecrets) bool {
		return cutil.FormExchangeIdWithSpecRef(e.MsInstUrl) == cutil.FormExchangeIdWithSpecRef(serviceUrl)
	}
}

func OrgSecFilter(serviceOrg string) SecFilter {
	return func(e PersistedServiceSecrets) bool { return e.MsInstOrg == serviceOrg }
}

func VersRangeSecFilter(versRange string) SecFilter {
	return func(e PersistedServiceSecrets) bool {
		if versExp, err := semanticversion.Version_Expression_Factory(e.MsInstVers); err != nil {
			return false
		} else if inRange, err := versExp.Is_within_range(versRange); err != nil {
			return false
		} else {
			return inRange
		}
	}
}

func FindAllServiceSecretsWithFilters(db AgentDatabase, filters []SecFilter) ([]PersistedServiceSecrets, error) {
	return db.FindAllServiceSecretsWithFilters(filters)
}

func FindAllServiceSecretsWithSpecs(db AgentDatabase, svcUrl string, svcOrg string) ([]PersistedServiceSecrets, error) {
	filters := []SecFilter{UrlSecFilter(svcUrl), OrgSecFilter(svcOrg)}

	return FindAllServiceSecretsWithFilters(db, filters)
}

// Returns the secret from the db if it was there. No error returned if it is not in the db
func DeleteSecrets(db AgentDatabase, secName string, msInstId string) (*PersistedServiceSecret, error) {
	return db.DeleteSecrets(secName, msInstId)
}

func DeleteSecretsSpec(db AgentDatabase, secName string, svcName string, svcOrg string, svcVersionRange string) error {
	if allServiceSecrets, err := FindAllServiceSecretsWithSpecs(db, svcName, svcOrg); err != nil {
		return err
	} else {
		for _, dbServiceSecrets := range allServiceSecrets {
			if _, ok := dbServiceSecrets.SecretsMap[secName]; ok {
				delete(dbServiceSecrets.SecretsMap, secName)
				if err = SaveAllSecretsForService(db, dbServiceSecrets.MsInstKey, &dbServiceSecrets); err != nil {
					glog.Errorf("Failed to delete secret %v for service %v from db: %v", dbServiceSecrets.MsInstKey, secName, err)
				}
			}
		}
	}
	return nil
}
