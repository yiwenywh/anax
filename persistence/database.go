package persistence

import (
	"github.com/open-horizon/anax/config"
	"github.com/open-horizon/anax/policy" // needed??
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
	UpdateAttribute(attributeid string, *Attribute) (*Attribute, error)
	DeleteAttribute(attributeid string) (*Attribute, error)

}

