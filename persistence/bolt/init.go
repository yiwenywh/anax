package bolt

import (
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"github.com/golang/glog"
	"github.com/open-horizon/anax/config"
	"github.com/open-horizon/anax/persistence"
	"os"
	"path"
	"time"
)

func init() {   // TODO: is this the right place to do init?
	persistence.Register("bolt", new(AgentBoltDB))
}

// TOOD: is this the right place to define AgentBoltDB?
// This is the object that represents the handle to the bolt func (db *AgentBoltDB)
type AgentBoltDB struct {
	db *bolt.DB
}

// Setup everything bolt DB needs to be able to run an agbot. Since bolt is a simple document based database,
// all we need to setup is database file itself. There are no tables or indexes to create for bolt DB.
func (db *AgentBoltDB) Initialize(cfg *config.HorizonConfig) error {

	fmt.Sprintf("cfg.Edge.DBPath is %v", cfg.Edge.DBPath)
	if err := os.MkdirAll(cfg.Edge.DBPath, 0700); err != nil {
		return errors.New(fmt.Sprintf("unable to create directory %v for agent bolt DB configuration, error: %v", cfg.Edge.DBPath, err))
	}

	dbname := path.Join(cfg.Edge.DBPath, "anax.db")

	if agentdb, err := bolt.Open(dbname, 0600, &bolt.Options{Timeout: 10 * time.Second}); err != nil {
		return errors.New(fmt.Sprintf("unable to open agent bolt database %v, error: %v", dbname, err))
	} else {
		db.db = agentdb

	}

	// Initialize the one and only search session object
	//if err := db.InitSearchSession(); err != nil {
	//	return errors.New(fmt.Sprintf("unable to init search session object in database %v, error: %v", dbname, err))
	//}

	return nil

}

func (db *AgentBoltDB) Close() {
	glog.V(2).Infof("Closing bolt database")
	db.db.Close()
	glog.V(2).Infof("Closed bolt database")
}
