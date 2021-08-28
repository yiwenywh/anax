package persistence

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"strings"
	"time"
)

const DEVICES = "devices"

// device types
const DEVICE_TYPE_DEVICE = "device"
const DEVICE_TYPE_CLUSTER = "cluster"

const CONFIGSTATE_UNCONFIGURING = "unconfiguring"
const CONFIGSTATE_UNCONFIGURED = "unconfigured"
const CONFIGSTATE_CONFIGURING = "configuring"
const CONFIGSTATE_CONFIGURED = "configured"

type Configstate struct {
	State          string `json:"state"`
	LastUpdateTime uint64 `json:"last_update_time"`
}

func (c Configstate) String() string {
	return fmt.Sprintf("State: %v, Time: %v", c.State, c.LastUpdateTime)
}

// This function returns the pattern org, pattern name and formatted pattern string 'pattern org/pattern name'.
// If the input pattern does not contain the org name, the device org name will be used as the pattern org name.
// The input is a pattern string 'pattern org/pattern name' or just 'pattern name' for backward compatibility.
// The device org is the org name for the device.
func GetFormatedPatternString(pattern string, device_org string) (string, string, string) {
	if pattern == "" {
		return "", "", ""
	} else if ix := strings.Index(pattern, "/"); ix < 0 {
		if device_org == "" {
			return device_org, pattern, pattern
		} else {
			return device_org, pattern, fmt.Sprintf("%v/%v", device_org, pattern)
		}
	} else {
		return pattern[:ix], pattern[ix+1:], pattern
	}
}

type ExchangeDevice struct {
	Id                 string      `json:"id"`
	Org                string      `json:"organization"`
	Pattern            string      `json:"pattern"`
	Name               string      `json:"name"`
	NodeType           string      `json:"nodeType"`
	Token              string      `json:"token"`
	TokenLastValidTime uint64      `json:"token_last_valid_time"`
	TokenValid         bool        `json:"token_valid"`
	HA                 bool        `json:"ha"`
	Config             Configstate `json:"configstate"`
}

func (e ExchangeDevice) String() string {
	var tokenShadow string
	if e.Token != "" {
		tokenShadow = "set"
	} else {
		tokenShadow = "unset"
	}

	return fmt.Sprintf("Org: %v, Token: <%s>, Name: %v, NodeType: %v, TokenLastValidTime: %v, TokenValid: %v, Pattern: %v, %v", e.Org, tokenShadow, e.Name, e.NodeType, e.TokenLastValidTime, e.TokenValid, e.Pattern, e.Config)
}

func (e ExchangeDevice) GetId() string {
	return fmt.Sprintf("%v/%v", e.Org, e.Id)
}

func newExchangeDevice(id string, token string, name string, nodeType string, tokenLastValidTime uint64, ha bool, org string, pattern string, configstate string) (*ExchangeDevice, error) {
	if id == "" || token == "" || name == "" || tokenLastValidTime == 0 || org == "" {
		return nil, errors.New("Cannot create exchange device, illegal arguments")
	}

	cfg := Configstate{
		State:          configstate,
		LastUpdateTime: uint64(time.Now().Unix()),
	}

	// make the pattern to the standard "org/pattern" format
	if pattern != "" {
		_, _, pat := GetFormatedPatternString(pattern, org)
		pattern = pat
	}

	return &ExchangeDevice{
		Id:                 id,
		Name:               name,
		NodeType:           nodeType,
		Token:              token,
		TokenLastValidTime: tokenLastValidTime,
		TokenValid:         true,
		HA:                 ha,
		Org:                org,
		Pattern:            pattern,
		Config:             cfg,
	}, nil
}

func (e *ExchangeDevice) GetNodeType() string {
	if e.NodeType == "" {
		return DEVICE_TYPE_DEVICE
	} else {
		return e.NodeType
	}
}

func (e *ExchangeDevice) IsEdgeCluster() bool {
	return e.NodeType == DEVICE_TYPE_CLUSTER
}

// a convenience function b/c we know there is really only one device
func (e *ExchangeDevice) InvalidateExchangeToken(db AgentDatabase) (*ExchangeDevice, error) {
	exchDev, err := FindExchangeDevice(db)
	if err != nil {
		return nil, err
	}

	return updateExchangeDevice(db, e, exchDev.Id, true, func(d ExchangeDevice) *ExchangeDevice {
		d.Token = ""
		return &d
	})
}

func (e *ExchangeDevice) SetExchangeDeviceToken(db AgentDatabase, deviceId string, token string) (*ExchangeDevice, error) {
	if deviceId == "" || token == "" {
		return nil, errors.New("Argument null and mustn't be")
	}

	return updateExchangeDevice(db, e, deviceId, false, func(d ExchangeDevice) *ExchangeDevice {
		d.Token = token
		return &d
	})
}

func (e *ExchangeDevice) SetConfigstate(db AgentDatabase, deviceId string, state string) (*ExchangeDevice, error) {
	if deviceId == "" || state == "" {
		return nil, errors.New("Argument null and mustn't be")
	}

	return updateExchangeDevice(db, e, deviceId, false, func(d ExchangeDevice) *ExchangeDevice {
		d.Config.State = state
		d.Config.LastUpdateTime = uint64(time.Now().Unix())
		return &d
	})
}

func (e *ExchangeDevice) SetNodeType(db AgentDatabase, deviceId string, nodeType string) (*ExchangeDevice, error) {
	if deviceId == "" || nodeType == "" {
		return nil, errors.New("The argument deviceId or nodeType cannot be empty.")
	}

	return updateExchangeDevice(db, e, deviceId, false, func(d ExchangeDevice) *ExchangeDevice {
		d.NodeType = nodeType
		return &d
	})
}

func (e *ExchangeDevice) SetPattern(db AgentDatabase, deviceId string, pattern string) (*ExchangeDevice, error) {
	if deviceId == "" {
		return nil, errors.New("Argument null and mustn't be")
	}

	return updateExchangeDevice(db, e, deviceId, false, func(d ExchangeDevice) *ExchangeDevice {
		d.Pattern = pattern
		return &d
	})
}

func (e *ExchangeDevice) IsState(state string) bool {
	return e.Config.State == state
}

func updateExchangeDevice(db AgentDatabase, self *ExchangeDevice, deviceId string, invalidateToken bool, fn func(d ExchangeDevice) *ExchangeDevice) (*ExchangeDevice, error) {
	return db.UpdateExchangeDevice(self, deviceId, invalidateToken, fn)
}

// always assumed the given token is valid at the time of call
func SaveNewExchangeDevice(db AgentDatabase, id string, token string, name string, nodeType string, ha bool, organization string, pattern string, configstate string) (*ExchangeDevice, error) {
	return db.SaveNewExchangeDevice(id, token, name, nodeType, ha, organization, pattern, configstate)
}

func FindExchangeDevice(db AgentDatabase) (*ExchangeDevice, error) {
	return db.FindExchangeDevice()
}

func DeleteExchangeDevice(db AgentDatabase) error {
	return db.DeleteExchangeDevice()
}

// Migrate a device object if it is restarted ona newer level of code.
func MigrateExchangeDevice(db AgentDatabase) (bool, error) {
	usingPattern := false
	// If the device object already exists, make sure its service or workload mode is set correctly. If not, set it.
	// This code handles devices that upgrade to an anax runtime that supports service mode but the device is still
	// using workloads.
	if db != nil {
		if dev, _ := FindExchangeDevice(db); dev != nil {

			// If the existing device is using a pattern then we need to turn off agreement tracking when we create the policy manager.
			if dev.Pattern != "" {
				usingPattern = true
			}
		}
	}
	return usingPattern, nil
}
