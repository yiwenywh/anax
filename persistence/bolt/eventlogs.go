package bolt

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	bolt "go.etcd.io/bbolt"
	"github.com/open-horizon/anax/i18n"
	"golang.org/x/text/message"
	"strconv"
)

// save the timestamp for the last unregistration into db.
func (db *AgentBoltDB) SaveLastUnregistrationTime(last_unreg_time uint64) error {
	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		if bucket, err := tx.CreateBucketIfNotExists([]byte(LAST_UNREG)); err != nil {
			return err
		} else {
			return bucket.Put([]byte("lastunreg"), []byte(strconv.FormatUint(last_unreg_time, 10)))
		}
	})

	return writeErr
}

// get the timestamp for the last unregistration from db.
func (db *AgentBoltDB) GetLastUnregistrationTime() (uint64, error) {
	var last_unreg uint64
	last_unreg = 0

	// fetch event logs
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(LAST_UNREG)); b != nil {
			v := b.Get([]byte("lastunreg"))
			if s, err := strconv.ParseUint(string(v[:]), 10, 64); err != nil {
				return fmt.Errorf("Failed to convert the last unregistration time %v into uint64, error: %v", v, err)
			} else {
				last_unreg = s
			}
		}

		return nil // end the transaction
	})

	if readErr != nil {
		return 0, readErr
	} else {
		return last_unreg, nil
	}
}

// save the event log record into db.
func (db *AgentBoltDB) SaveEventLog(event_log *EventLog) error {
	writeErr := db.db.Update(func(tx *bolt.Tx) error {
		if bucket, err := tx.CreateBucketIfNotExists([]byte(EVENT_LOGS)); err != nil {
			return err
		} else if nextKey, err := bucket.NextSequence(); err != nil {
			return fmt.Errorf("Unable to get sequence key for new event log %v. Error: %v", event_log, err)
		} else {
			strKey := strconv.FormatUint(nextKey, 10)
			event_log.Id = strKey

			serial, err := json.Marshal(*event_log)
			if err != nil {
				return fmt.Errorf("Failed to serialize the event log: %v. Error: %v", *event_log, err)
			}
			return bucket.Put([]byte(strKey), serial)
		}
	})

	NewErrorLog(db, *event_log)
	return writeErr
}

// Find the event log from the db
func (db *AgentBoltDB) FindEventLogWithKey(key string) (*EventLog, error) {
	var pel *EventLog
	pel = nil

	// fetch event logs
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(EVENT_LOGS)); b != nil {
			v := b.Get([]byte(key))

			var el EventLogRaw

			if err := json.Unmarshal(v, &el); err != nil {
				glog.Errorf("Unable to deserialize event log db record: %v. Error: %v", v, err)
				return err
			} else {
				if esrc, err := GetRealEventSource(el.SourceType, el.Source); err != nil {
					glog.Errorf("Unable to convert event source: %v. Error: %v", el.Source, err)
					return err
				} else {
					pel = newEventLog1(el.Severity, el.Message, el.MessageMeta, el.EventCode, el.SourceType, *esrc)
					pel.Id = el.Id
					pel.Timestamp = el.Timestamp
					return nil
				}
			}
		}

		return nil // end the transaction
	})

	if readErr != nil {
		return nil, readErr
	} else {
		return pel, nil
	}
}

// find event logs from the db for the given filters
func (db *AgentBoltDB) FindEventLogs(filters []EventLogFilter) ([]EventLog, error) {
	evlogs := make([]EventLog, 0)

	// fetch logs
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(EVENT_LOGS)); b != nil {
			b.ForEach(func(k, v []byte) error {

				var el EventLogRaw

				if err := json.Unmarshal(v, &el); err != nil {
					glog.Errorf("Unable to deserialize event log db record: %v. Error: %v", v, err)
				} else {
					if esrc, err := GetRealEventSource(el.SourceType, el.Source); err != nil {
						glog.Errorf("Unable to convert event source: %v. Error: %v", el.Source, err)
					} else {
						pel := newEventLog1(el.Severity, el.Message, el.MessageMeta, el.EventCode, el.SourceType, *esrc)
						pel.Id = el.Id
						pel.Timestamp = el.Timestamp

						exclude := false
						for _, filterFn := range filters {
							if !filterFn(*pel) {
								exclude = true
							}
						}
						if !exclude {
							evlogs = append(evlogs, *pel)
						}
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
		return evlogs, nil
	}
}

// find event logs from the db for the given given selectors.
// If all_logs is false, only the event logs for the current registration is returned.
func (db *AgentBoltDB) FindEventLogsWithSelectors(all_logs bool, selectors map[string][]Selector, msgPrinter *message.Printer) ([]EventLog, error) {
	// separate base selectors from the source selectors
	base_selectors, source_selectors := persistence.GroupSelectors(selectors)

	evlogs := make([]EventLog, 0)

	last_unreg := uint64(0)
	if !all_logs {
		if l, err := GetLastUnregistrationTime(db); err != nil {
			return nil, fmt.Errorf("Faild to get the last unregistration time stamp from db. %v", err)
		} else {
			last_unreg = l
		}
	}

	if msgPrinter == nil {
		msgPrinter = i18n.GetMessagePrinter()
	}

	// fetch logs
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(EVENT_LOGS)); b != nil {
			b.ForEach(func(k, v []byte) error {

				var el EventLogRaw

				if err := json.Unmarshal(v, &el); err != nil {
					glog.Errorf("Unable to deserialize event log db record: %v. Error: %v", v, err)
				} else {
					// Use the given message printer to translate the message saved in MessageMeta and save it to Message.
					if el.MessageMeta != nil && el.MessageMeta.MessageKey != "" {
						el.Message = msgPrinter.Sprintf(el.MessageMeta.MessageKey, el.MessageMeta.MessageArgs...)
						// set MessageMeta to nil so that it will not get displayed.
						el.MessageMeta = nil
					}

					if (all_logs || el.Timestamp > last_unreg) && el.EventLogBase.Matches(base_selectors) {
						if esrc, err := GetRealEventSource(el.SourceType, el.Source); err != nil {
							glog.Errorf("Unable to convert event source: %v. Error: %v", el.Source, err)
						} else if (*esrc).Matches(source_selectors) {
							pel := newEventLog1(el.Severity, el.Message, el.MessageMeta, el.EventCode, el.SourceType, *esrc)
							pel.Id = el.Id
							pel.Timestamp = el.Timestamp
							evlogs = append(evlogs, *pel)
						}
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
		return evlogs, nil
	}
}

// find all event logs from the db
func (db *AgentBoltDB) FindAllEventLogs() ([]EventLog, error) {
	evlogs := make([]EventLog, 0)

	// fetch logs
	readErr := db.db.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket([]byte(EVENT_LOGS)); b != nil {
			b.ForEach(func(k, v []byte) error {

				var el EventLogRaw

				if err := json.Unmarshal(v, &el); err != nil {
					glog.Errorf("Unable to deserialize event log db record: %v. Error: %v", v, err)
				} else {
					if esrc, err := GetRealEventSource(el.SourceType, el.Source); err != nil {
						glog.Errorf("Unable to convert event source: %v. Error: %v", el.Source, err)
					} else {
						pel := newEventLog1(el.Severity, el.Message, el.MessageMeta, el.EventCode, el.SourceType, *esrc)
						pel.Id = el.Id
						pel.Timestamp = el.Timestamp

						evlogs = append(evlogs, *pel)
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
		return evlogs, nil
	}
}
