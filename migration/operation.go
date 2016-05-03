package migration

import (
	"database/sql"
	"log"

	"github.com/blendlabs/spiffy"
)

// Step is an alias to NewOperation.
func Step(action Action, body Statement, args ...string) Migration {
	return NewOperation(action, body, args...)
}

// NewOperation creates a new invocable.
func NewOperation(action Action, body Statement, args ...string) Migration {
	return &Operation{
		Action: action,
		Body:   body,
		Args:   args,
	}
}

// Operation is a closure for a Operation
type Operation struct {
	Logger *log.Logger
	Action Action
	Body   Statement
	Args   []string
}

// SetLogger sets the logger.
func (s *Operation) SetLogger(logger *log.Logger) {
	s.Logger = logger
}

// Test wraps the action in a commit.
func (s Operation) Test(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return
	}
	defer func() {
		err = tx.Rollback()
	}()
	return s.Invoke(c, tx)
}

// Apply wraps the action in a commit.
func (s Operation) Apply(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return
	}
	defer func() {
		err = tx.Commit()
	}()
	return s.Invoke(c, tx)
}

// Invoke runs the operation against the given connection and transaction.
func (s Operation) Invoke(c *spiffy.DbConnection, tx *sql.Tx) error {
	return s.Action(s.Logger, c, tx, s.Body, s.Args...)
}
