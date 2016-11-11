package spiffy

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	logger "github.com/blendlabs/go-logger"
)

const (
	// EventFlagExecute is a logger.EventFlag
	EventFlagExecute logger.EventFlag = "spiffy.execute"

	// EventFlagQuery is a logger.EventFlag
	EventFlagQuery logger.EventFlag = "spiffy.query"

	explainAnalyze    = "EXPLAIN ANALYZE"
	explainMinElapsed = 1 * time.Second
)

// NewLoggerEventListener returns a new listener for diagnostics events.
func NewLoggerEventListener(action func(writer logger.Logger, ts logger.TimeSource, flag logger.EventFlag, query string, elapsed time.Duration, err error)) logger.EventListener {
	return func(writer logger.Logger, ts logger.TimeSource, eventFlag logger.EventFlag, state ...interface{}) {
		if state[2] != nil {
			action(writer, ts, eventFlag, state[0].(string), state[1].(time.Duration), state[2].(error))
		} else {
			action(writer, ts, eventFlag, state[0].(string), state[1].(time.Duration), nil)
		}
	}
}

// Event is a wrapper for the underlying Event datatype
type Event struct {
	Title string
	Text  string
}

// NewEvent creates a new Event
func NewEvent(title, text string) *Event {
	return &Event{
		Title: title,
		Text:  text,
	}
}

type queryExplanation struct {
	statement   string
	explanation []byte
	err         error
}

func (qe *queryExplanation) asEvent() (*Event, error) {
	if qe.err != nil {
		return nil, qe.err
	}
	eventTitle := fmt.Sprintf("Duration of database query/execute over threshold: %s", explainMinElapsed)
	eventDescription := fmt.Sprintf("`%s`\n %s", qe.statement, qe.explanation)
	event := NewEvent(eventTitle, eventDescription)
	return event, nil
}

type explanationRow struct {
	QueryPlan string `db:"QUERY PLAN"`
}

func newQueryExplanation(statement string) *queryExplanation {
	explanationRows := []explanationRow{}
	explainQueryString := fmt.Sprintf("%s %s", explainAnalyze, statement)
	err := DefaultDb().Query(explainQueryString).OutMany(&explanationRows)
	if err != nil {
		return &queryExplanation{
			err: err,
		}
	}
	var lines bytes.Buffer
	for _, result := range explanationRows {
		lines.WriteString(result.QueryPlan)
		lines.WriteString("\n")
	}
	return &queryExplanation{
		explanation: lines.Bytes(),
		statement:   statement,
	}
}

func isAutoExplainStatement(statement string) bool {
	return strings.HasPrefix(statement, explainAnalyze)
}

func addStatementEventListener(diagnostics *logger.DiagnosticsAgent, listener logger.EventListener) {
	diagnostics.EnableEvent(EventFlagExecute)
	diagnostics.AddEventListener(EventFlagExecute, listener)
	diagnostics.EnableEvent(EventFlagQuery)
	diagnostics.AddEventListener(EventFlagQuery, listener)
}

// AddAutoExplainListener invokes its callback with the output of EXPLAIN ANALYZE for long running SQL queries
func AddAutoExplainListener(diagnostics *logger.DiagnosticsAgent, listener func(*Event) error) {
	if diagnostics == nil {
		return
	}
	addStatementEventListener(diagnostics, func(writer logger.Logger, ts logger.TimeSource, eventFlag logger.EventFlag, data ...interface{}) {
		statement, elapsed := data[0].(string), data[1].(time.Duration)
		// 1. Only EXPLAIN ANALYZE queries that took longer than threshold
		// 2. Don't call EXPLAIN ANALYZE on statements beginning with EXPLAIN ANALYZE...
		if elapsed >= explainMinElapsed && !isAutoExplainStatement(statement) {
			logger.WriteEventf(writer, ts, eventFlag, logger.ColorYellow, "Duration of SQL statement: %v", elapsed)
			event, err := newQueryExplanation(statement).asEvent()
			if err != nil {
				diagnostics.Error(err)
				return
			}
			err = listener(event)
			if err != nil {
				diagnostics.Error(err)
				return
			}
		}
	})
}
