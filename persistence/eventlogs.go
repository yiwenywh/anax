package persistence

import (
	"fmt"
	"encoding/json"
	"github.com/open-horizon/anax/i18n"
	"golang.org/x/text/message"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// event log table name
const EVENT_LOGS = "event_logs"

// table stores the timestamp of last unregistration
const LAST_UNREG = "last_unreg"

const BASE_SELECTORS = "source_type,severity,message,event_code,record_id,timestamp" // only support these 2 for now

// Each event source implements this interface.
type EventSourceInterface interface {

	//Used for searching. The input is a map of selector array.
	// The input is a map of selector array.
	// For example:
	//   selectors = [string][]Selector{
	//			"a": [{"~": "test"}, {"~", "agreement"}],
	//          "b": [{"=", "this is a test"}],
	//			"c":[{">", 100}]
	//		}
	// It means checking if this event source matches the following logic:
	//  the attribute "a" contains the word "test" and "agreement",
	//  attribute "b" equals "this is a test" and attribute "c" is greater than 100.
	Matches(map[string][]Selector) bool
}

type EventLogBase struct {
	Id          string       `json:"record_id"` // unique primary key for records
	Timestamp   uint64       `json:"timestamp"`
	Severity    string       `json:"severity"` // info, warning or error
	Message     string       `json:"message"`  // obsolte in DB, used for backward compatibility and for output
	EventCode   string       `json:"event_code"`
	SourceType  string       `json:"source_type"`            // the type of the source. It can be agreement, service, image, workload etc.
	MessageMeta *MessageMeta `json:"message_meta,omitempty"` // the message and it's arguements for fmt.Sprintf. This is used for i18n.
}

// Checks if the base event log matches the selectors
func (w EventLogBase) Matches(selectors map[string][]Selector) bool {
	for s_attr, s_vals := range selectors {
		var attr interface{}
		switch s_attr {
		case "source_type":
			attr = w.SourceType
		case "severity":
			attr = w.Severity
		case "message":
			attr = w.Message
		case "event_code":
			attr = w.EventCode
		case "record_id":
			attr = w.Id
		case "timestamp":
			attr = w.Timestamp
		default:
			return false // not tolerate wrong attribute name in the selector
		}

		m, _, _ := MatchAttributeValue(attr, s_vals)
		if !m {
			return false
		}
	}

	return true
}

type EventLog struct {
	EventLogBase
	Source EventSourceInterface `json:"event_source"` // source involved for this event.
}

func NewEventLog(severity string, message_meta *MessageMeta, event_code string, source_type string, source EventSourceInterface) *EventLog {
	return &EventLog{
		EventLogBase: EventLogBase{
			Timestamp:   uint64(time.Now().Unix()),
			Severity:    severity,
			EventCode:   event_code,
			SourceType:  source_type,
			MessageMeta: message_meta,
		},
		Source: source,
	}
}

func newEventLog1(severity string, message string, message_meta *MessageMeta, event_code string, source_type string, source EventSourceInterface) *EventLog {
	return &EventLog{
		EventLogBase: EventLogBase{
			Timestamp:   uint64(time.Now().Unix()),
			Severity:    severity,
			Message:     message,
			EventCode:   event_code,
			SourceType:  source_type,
			MessageMeta: message_meta,
		},
		Source: source,
	}
}

func (w EventLog) String() string {
	mm := ""
	if w.MessageMeta != nil {
		mm = fmt.Sprintf("%v", w.MessageMeta)
	}

	return fmt.Sprintf("ID: %v, "+
		"Timestamp: %v, "+
		"Severity: %v, "+
		"Message: %v, "+
		"MessageMeta: %v, "+
		"EventCode: %v, "+
		"SourceType: %v, "+
		"Source: %v",
		w.Id, w.Timestamp, w.Severity, w.Message, mm, w.EventCode, w.SourceType, w.Source)
}

func (w EventLog) ShortString() string {
	return w.String()
}

// Checks the source type of the event log. An eventlog can belong to different source types at the same time.
func (w EventLog) IsSourceType(source_type string) bool {
	return w.SourceType == source_type
}

// Checks if the event log matches the selectors
// For example:
//   selectors = [string][]Selector{
//			"a": [{"~": "test"}, {"~", "agreement"}],
//          "b": [{"=", "this is a test"}],
//			"c":[{">", 100}]
//		}
// It means checking if this event log matches the following logic:
//  the attribute "a" contains the word "test" and "agreement",
//  and attribute "b" equals "this is a test" and attribute "c" is greater than 100.
func (w EventLog) Matches(selectors map[string][]Selector) bool {

	// separate base selectors and source selectors
	base_selectors, source_selectiors := GroupSelectors(selectors)

	// match the base attributes first.
	if !w.EventLogBase.Matches(base_selectors) {
		return false
	}

	// then match the source
	return w.Matches2(base_selectors, source_selectiors)
}

// Checks if the event log matches the base selectors and source selectors
func (w EventLog) Matches2(base_selectors map[string][]Selector, source_selectors map[string][]Selector) bool {

	// match the base attributes first.
	if !w.EventLogBase.Matches(base_selectors) {
		return false
	}

	// then match the source
	return w.Source.Matches(source_selectors)
}

// This structure is used to store the message key and args for fmt.Sprintf().
// It can be used by the MessagePrinter.Sprintf to print out the messages for different locales.
type MessageMeta struct {
	MessageKey  string        `json:"message_key"`
	MessageArgs []interface{} `json:"message_args,omitempty"`
}

func (w MessageMeta) String() string {
	return fmt.Sprintf("MessageKey: %v, MessageArgs: %v", w.MessageKey, w.MessageArgs)
}

func NewMessageMeta(msg_key string, message_args ...interface{}) *MessageMeta {
	return &MessageMeta{
		MessageKey:  msg_key,
		MessageArgs: message_args,
	}
}

// save the timestamp for the last unregistration into db.
func SaveLastUnregistrationTime(db AgentDatabase, last_unreg_time uint64) error {
	return db.SaveLastUnregistrationTime(last_unreg_time)
}

// Find the event log from the db
func GetLastUnregistrationTime(db AgentDatabase) (uint64, error) {
	return db.GetLastUnregistrationTime()
}

// save the event log record into db.
func SaveEventLog(db AgentDatabase, event_log *EventLog) error {
	return db.SaveEventLog(event_log)
}

type EventLogRaw struct {
	EventLogBase
	Source *json.RawMessage `json:"event_source"` // source involved for this event.
}

// return the real event source from the base event source
func GetRealEventSource(source_type string, src *json.RawMessage) (*EventSourceInterface, error) {

	var ret_src EventSourceInterface

	switch source_type {
	case SRC_TYPE_AG:
		var ag_src AgreementEventSource
		if err := json.Unmarshal(*src, &ag_src); err != nil {
			return nil, err
		}
		ret_src = ag_src
	case SRC_TYPE_SVC:
		var svc_src ServiceEventSource
		if err := json.Unmarshal(*src, &svc_src); err != nil {
			return nil, err
		}
		ret_src = svc_src
	case SRC_TYPE_NODE:
		var node_src NodeEventSource
		if err := json.Unmarshal(*src, &node_src); err != nil {
			return nil, err
		}
		ret_src = node_src
	case SRC_TYPE_DB:
		var db_src DatabaseEventSource
		if err := json.Unmarshal(*src, &db_src); err != nil {
			return nil, err
		}
		ret_src = db_src
	case SRC_TYPE_EXCH:
		var ex_src ExchangeEventSource
		if err := json.Unmarshal(*src, &ex_src); err != nil {
			return nil, err
		}
		ret_src = ex_src

	default:
		return nil, fmt.Errorf("Unknown event source type: %v", source_type)
	}

	return &ret_src, nil
}

// Find the event log from the db
func FindEventLogWithKey(db AgentDatabase, key string) (*EventLog, error) {
	return db.FindEventLogWithKey(key)
}

// filter on EventLog
type EventLogFilter func(EventLog) bool

// filter on severity
func SeverityELFilter(severity string) EventLogFilter {
	return func(e EventLog) bool { return e.Severity == severity }
}

// filter on source type
func SourceTypeELFilter(source_type string) EventLogFilter {
	return func(e EventLog) bool { return e.IsSourceType(source_type) }
}

// filter on the source type and value
func SelectorFilter(selectors map[string][]Selector) EventLogFilter {
	return func(e EventLog) bool { return e.Matches(selectors) }
}

// find event logs from the db for the given filters
func FindEventLogs(db AgentDatabase, filters []EventLogFilter) ([]EventLog, error) {
	return db.FindEventLogs(filters)
}

// find event logs from the db for the given given selectors.
// If all_logs is false, only the event logs for the current registration is returned.
func FindEventLogsWithSelectors(db AgentDatabase, all_logs bool, selectors map[string][]Selector, msgPrinter *message.Printer) ([]EventLog, error) {
	return db.FindEventLogsWithSelectors(all_logs, selectors, msgPrinter)
}

// find all event logs from the db
func FindAllEventLogs(db AgentDatabase) ([]EventLog, error) {
	return db.FindAllEventLogs()
}

type Selector struct {
	Op         string
	MatchValue interface{}
}

// convert the given string to a selector
func ConvertToSelectorType(s string) (*Selector, error) {
	var a interface{}
	op := "="
	a = s

	handled := false
	if len(s) > 1 {
		switch s[0] {
		case '~':
			op = "~"
			a = s[1:]
			handled = true
		case '>', '<':
			// parse to float64 because it can represent any other number types such as int, uint, int64 etc.
			if num, err := strconv.ParseFloat(s[1:], 64); err == nil {
				op = string(s[0])
				a = num
			} else {
				// '>' and '<' will be used for string comparison.
				op = string(s[0])
				a = s[1:]
			}
			handled = true
		}
	}

	if !handled {
		if s == "true" || s == "TRUE" { // a boolean
			a = true
		} else if s == "false" || s == "FALSE" { // a boolean
			a = false
		} else if num, err := strconv.ParseFloat(s, 64); err == nil { // a number
			a = num
		}
	}

	return &Selector{Op: op, MatchValue: a}, nil
}

// convert the given http.Request.Form into map of Selectors
func ConvertToSelectors(selections map[string][]string) (map[string][]Selector, error) {
	s_map := make(map[string][]Selector)
	for attr, vals := range selections {
		s_array := make([]Selector, 0)
		for _, v := range vals {
			s_v, err := ConvertToSelectorType(v)
			if err != nil {
				return nil, err
			} else {
				s_array = append(s_array, *s_v)
			}
		}

		s_map[attr] = s_array
	}
	return s_map, nil
}

// This function separates base selectors and source selectors.
// It returns (base_selectors, source_selectors)
func GroupSelectors(selectors map[string][]Selector) (map[string][]Selector, map[string][]Selector) {
	base_selectors := make(map[string][]Selector)
	source_selectiors := make(map[string][]Selector)

	for attr, val := range selectors {
		found := false
		for _, base_attr := range strings.Split(BASE_SELECTORS, ",") {
			if attr == base_attr {
				found = true
				break
			}
		}

		if found {
			base_selectors[attr] = val
		} else {
			source_selectiors[attr] = val
		}
	}

	return base_selectors, source_selectiors
}

// Given the selector, check if the given attribute match or not.
// Example:
//   MatchTypes("this is a test", [{ "~", "test"}, {"~", "aaa"}])
//     --- check the string to see if it contains "test" and "aaa".
//   MatchTypes(12345, [{">", 100}])
//     --- check the integer to see if it is greater than 100.
// This function returns (match_or_not, handled_or_not, error)
func MatchAttributeValue(attr interface{}, selectors []Selector) (bool, bool, error) {
	for _, s := range selectors {
		switch s.MatchValue.(type) {
		case int, uint, int32, int64, uint64, float32, float64:

			// convert the two parties into float64 because it can represent all types of numbers
			var a_data, s_data float64
			var err error

			if reflect.TypeOf(attr).Kind() != reflect.Float64 {
				if a_data, err = strconv.ParseFloat(fmt.Sprintf("%v", attr), 64); err != nil {
					return false, true, fmt.Errorf("Error converting %v to float64: %v", attr, err)
				}
			} else {
				a_data = attr.(float64)
			}

			if reflect.TypeOf(s.MatchValue).Kind() != reflect.Float64 {
				if s_data, err = strconv.ParseFloat(fmt.Sprintf("%v", s.MatchValue), 64); err != nil {
					return false, true, fmt.Errorf("Error converting %v to float64: %v", s.MatchValue, err)
				}
			} else {
				s_data = s.MatchValue.(float64)
			}

			switch s.Op {
			case "=":
				if a_data != s_data {
					return false, true, nil
				}
			case ">":
				if a_data <= s_data {
					return false, true, nil
				}
			case "<":
				if a_data >= s_data {
					return false, true, nil
				}
			default:
				return false, true, fmt.Errorf("%v does not support operation: %v", reflect.TypeOf(attr).Kind(), s.Op)
			}

		case string:
			var a_data string
			if reflect.TypeOf(attr).Kind() != reflect.String {
				a_data = fmt.Sprintf("%v", attr)
			} else {
				a_data = attr.(string)
			}

			switch s.Op {
			case "=":
				if a_data != s.MatchValue.(string) {
					return false, true, nil
				}
			case "~":
				if !strings.Contains(a_data, s.MatchValue.(string)) {
					return false, true, nil
				}
			case ">":
				if !(a_data > s.MatchValue.(string)) {
					return false, true, nil
				}
			case "<":
				if !(a_data < s.MatchValue.(string)) {
					return false, true, nil
				}
			default:
				return false, true, fmt.Errorf("%v does not support operation: %v", reflect.TypeOf(attr).Kind(), s.Op)
			}

		case bool:
			switch attr.(type) {
			case bool:
				switch s.Op {
				case "=":
					if attr.(bool) != s.MatchValue.(bool) {
						return false, true, nil
					}
				default:
					return false, true, fmt.Errorf("Boolean does not support operation: %v", s.Op)
				}

			default:
				return false, true, fmt.Errorf("Selector %v type miss match.", s)
			}

		default:
			return false, false, nil
		}
	}

	return true, true, nil
}

// GetEventLogObject returns the full eventlog object associated with a given record id
func GetEventLogObject(db AgentDatabase, msgPrinter *message.Printer, recordID string) EventLog {
	if msgPrinter == nil {
		msgPrinter = i18n.GetMessagePrinter()
	}
	recordSelectorMap := make(map[string][]Selector)
	selector := []Selector{Selector{Op: "=", MatchValue: recordID}}
	recordSelectorMap["record_id"] = selector
	logs, err := FindEventLogsWithSelectors(db, true, recordSelectorMap, msgPrinter)
	if err != nil {
		return EventLog{}
	}
	if len(logs) == 0 {
		return EventLog{}
	}
	return logs[0]
}
