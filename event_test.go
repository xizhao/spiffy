package spiffy

import (
	"strings"
	"testing"
	"time"

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

	explanation, err := NewSlowStatementExplanation("SELECT * FROM test_table", time.Second)
	assert.Nil(err)

	text := "Statement: SELECT * FROM test_table\nDuration: 1s\nThreshold: 1s\nExplain Analyze:\nSeq Scan on test_table"
	assert.True(strings.HasPrefix(explanation.Description(), text))

	assert.Equal(explanation.Title(), "Slow SQL Statement (>1s)")
}

func TestSQLEventListeners(t *testing.T) {
	ch := make(chan int)
	diagnostics := logger.NewDiagnosticsAgentFromEnvironment()
	AddSlowStatementExplanationListener(diagnostics, func(*SlowStatementExplanation) error {
		go func() {
			ch <- 1
		}()
		return nil
	})
	diagnostics.OnEvent(EventFlagQuery, "SELECT * FROM test_table", time.Second, nil)
	<-ch
}
