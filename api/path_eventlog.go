package api

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/open-horizon/anax/eventlog"
	"github.com/open-horizon/anax/persistence"
	"golang.org/x/text/message"
	"sort"
)

// This API returns the event logs saved on the db.
func FindEventLogsForOutput(db persistence.AgentDatabase, all_logs bool, selections map[string][]string, msgPrinter *message.Printer) ([]persistence.EventLog, error) {

	glog.V(5).Infof(apiLogString(fmt.Sprintf("Getting event logs from the db. The selectors are: %v.", selections)))

	//convert to selectors
	s, err := persistence.ConvertToSelectors(selections)
	if err != nil {
		return nil, fmt.Errorf(msgPrinter.Sprintf("Error converting the selections into Selectors: %v", err))
	} else {
		glog.V(5).Infof(apiLogString(fmt.Sprintf("Converted selections into a map of persistence.Selector arrays: %v.", s)))
	}

	// get the event logs
	if event_logs, err := eventlog.GetEventLogs(db, all_logs, s, msgPrinter); err != nil {
		return nil, err
	} else {
		sort.Sort(EventLogByRecordId(event_logs))
		return event_logs, nil
	}
}

func FindSurfaceLogsForOutput(db persistence.AgentDatabase, msgPrinter *message.Printer) ([]persistence.SurfaceError, error) {
	outputLogs := make([]persistence.SurfaceError, 0)
	surfaceLogs, err := persistence.FindSurfaceErrors(db)
	if err != nil {
		return nil, err
	}
	for _, log := range surfaceLogs {
		if !log.Hidden {
			outputLogs = append(outputLogs, log)
		}
	}
	return outputLogs, nil
}
