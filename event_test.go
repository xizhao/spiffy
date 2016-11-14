package spiffy

import (
	"strings"
	"testing"
	"time"

	"sync"

	assert "github.com/blendlabs/go-assert"
	logger "github.com/blendlabs/go-logger"
)

func TestSlowStatementExplanation(t *testing.T) {
	assert := assert.New(t)
	tx, err := DefaultDb().Begin()
	defer tx.Rollback()
	assert.Nil(err)

	createTestTable := `CREATE TABLE IF NOT EXISTS test_table (id serial not null primary key);`
	err = DefaultDb().ExecInTx(createTestTable, tx)
	assert.Nil(err)

	explanation, err := NewSlowStatementExplanation("SELECT * FROM test_table", time.Second, time.Second, tx)
	assert.Nil(err)

	text := "Statement: SELECT * FROM test_table\nDuration: 1s\nThreshold: 1s\nExplain Analyze:\nSeq Scan on test_table"
	assert.True(strings.HasPrefix(explanation.Description(), text))
	assert.Equal(explanation.Title(), "Slow SQL Statement (>1s)")
}

func TestSQLEventListeners(t *testing.T) {
	assert := assert.New(t)
	tx, err := DefaultDb().Begin()
	defer tx.Rollback()
	assert.Nil(err)

	DefaultDb().IsolateToTransaction(tx)
	defer DefaultDb().ReleaseIsolation()

	wg := sync.WaitGroup{}
	wg.Add(1)

	createTestTable := `CREATE TABLE IF NOT EXISTS test_table (id serial not null primary key);`
	err = DefaultDb().ExecInTx(createTestTable, tx)
	assert.Nil(err)

	diagnostics := logger.NewDiagnosticsAgentFromEnvironment()
	diagnostics.EnableEvent(logger.EventError)
	diagnostics.EnableEvent(logger.EventFatalError)
	AddExplainSlowStatementsListener(diagnostics, func(*SlowStatementExplanation) error {
		defer wg.Done()
		return nil
	})

	diagnostics.OnEvent(EventFlagQuery, "SELECT * FROM test_table", defaultThreshold, nil)
	wg.Wait()
}
