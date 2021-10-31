package persistence

import (
	"errors"
	"fmt"
	"github.com/open-horizon/anax/config"
)

// The registry is a mechanism that enables optional persistence implementations to be plugged into the
// runtime. The implementation registers itself with this registry when the implementation's package init()
// method is driven. This mechanism prevents the need for the persistence package to import each of the
// optional DB specific packages. The only tricky part of this is that name of each DB implementation is hard
// coded here and in the implementation's call to the Register() method. Sharing constants would re-introduce
// the package dependency that we want to avoid.
type DatabaseProviderRegistry map[string]AgentDatabase

var DatabaseProviders = DatabaseProviderRegistry{}

func Register(name string, db AgentDatabase) {
	DatabaseProviders[name] = db
}

// Initialize the underlying Agent database depending on what is configured. If the bolt DB is configured, it is used.
// If nothing is configured, an error is returned.
func InitDatabase(cfg *config.HorizonConfig) (AgentDatabase, error) {

	if cfg.IsAgentBoltDBConfigured() {
		dbObj := DatabaseProviders["bolt"]
		return dbObj, dbObj.Initialize(cfg)

	}
	return nil, errors.New(fmt.Sprintf("Bolt DB is not configured correctly."))

}
