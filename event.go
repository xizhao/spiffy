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

	explainCommand   = "EXPLAIN"
	defaultThreshold = 250 * time.Millisecond
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

type explanationRow struct {
	QueryPlan string `db:"QUERY PLAN"`
}

// Explain runs EXPLAIN ANALYZE on a SQL statement and returns the output as a string
func Explain(statement string) (string, error) {
	explanationRows := []explanationRow{}
	explainQueryString := fmt.Sprintf("%s %s", explainCommand, statement)
	err := DefaultDb().Query(explainQueryString).OutMany(&explanationRows)
	if err != nil {
		return "", err
	}
	var lines bytes.Buffer
	for _, result := range explanationRows {
		lines.WriteString(result.QueryPlan)
		lines.WriteString("\n")
	}
	return lines.String(), nil
}

// SlowStatementExplanation wraps the output of EXPLAIN ANALYZE. It contains the statement, its duration, and a newline delimited explanation
type SlowStatementExplanation struct {
	statement   string
	explanation string
	duration    time.Duration
	threshold   time.Duration
}

// Title provides a brief description
func (e *SlowStatementExplanation) Title() string {
	return fmt.Sprintf("Slow SQL Statement (>%v)", e.threshold)
}

// Description provides a returns a multiline description of the explain analyze results
func (e *SlowStatementExplanation) Description() string {
	return fmt.Sprintf("Statement: %v\nDuration: %v\nThreshold: %v\nExplain Analyze:\n%v", e.statement, e.duration, e.threshold, e.explanation)
}

func (e *SlowStatementExplanation) String() string {
	return fmt.Sprintf("%s\n%s", e.Title(), e.Description())
}

// NewSlowStatementExplanation makes a new SlowStatementExplanation from a statement body and duration
func NewSlowStatementExplanation(statement string, duration time.Duration, threshold time.Duration) (*SlowStatementExplanation, error) {
	explanation, err := Explain(statement)
	if err != nil {
		return nil, err
	}
	return &SlowStatementExplanation{
		statement:   statement,
		explanation: explanation,
		duration:    duration,
		threshold:   threshold,
	}, nil
}

// AddStatementEventListener registers an EventListener to be invoked on every Query and Execute
func AddStatementEventListener(diagnostics *logger.DiagnosticsAgent, listener logger.EventListener) {
	diagnostics.EnableEvent(EventFlagExecute)
	diagnostics.AddEventListener(EventFlagExecute, listener)
	diagnostics.EnableEvent(EventFlagQuery)
	diagnostics.AddEventListener(EventFlagQuery, listener)
}

func isExplainStatement(statement string) bool {
	return strings.HasPrefix(statement, explainCommand)
}

// AddExplainSlowStatementsListener registers a callback to be called with an event containing the output of EXPLAIN ANALYZE for long running SQL queries
func AddExplainSlowStatementsListener(diagnostics *logger.DiagnosticsAgent, listener func(*SlowStatementExplanation) error, withThreshold ...func(string) time.Duration) {
	AddStatementEventListener(diagnostics, func(writer logger.Logger, ts logger.TimeSource, eventFlag logger.EventFlag, data ...interface{}) {
		statement, duration := data[0].(string), data[1].(time.Duration)
		threshold := defaultThreshold
		if len(withThreshold) > 0 {
			threshold = withThreshold[0](statement)
		}
		if duration >= threshold && !isExplainStatement(statement) {
			explanation, err := NewSlowStatementExplanation(statement, duration, threshold)
			if err != nil {
				diagnostics.Error(err)
				return
			}
			err = listener(explanation)
			if err != nil {
				diagnostics.Error(err)
				return
			}
		}
	})
}
