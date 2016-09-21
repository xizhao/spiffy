package migration

import (
	"database/sql"

	"github.com/blendlabs/go-exception"
	"github.com/blendlabs/spiffy"
)

// Step is an alias to NewOperation.
func Step(action Action, body Statement, args ...string) *Operation {
	return NewOperation(action, body, args...)
}

// NewOperation creates a new invocable.
func NewOperation(action Action, body Statement, args ...string) *Operation {
	return &Operation{
		action: action,
		body:   body,
		args:   args,
	}
}

// Operation is a closure for a Operation
type Operation struct {
	label  string
	parent *Runner
	logger *Logger
	action Action
	body   Statement
	args   []string
}

// Label returns the operation label.
func (o *Operation) Label() string {
	return o.label
}

// SetLabel sets the operation label.
func (o *Operation) SetLabel(label string) {
	o.label = label
}

// Parent returns the parent.
func (o *Operation) Parent() *Runner {
	return o.parent
}

// SetParent sets the operation parent.
func (o *Operation) SetParent(parent *Runner) {
	o.parent = parent
}

// SetLogger implements the migration method `SetLogger`.
func (o *Operation) SetLogger(logger *Logger) {
	o.logger = logger
}

// Test wraps the action in a transaction and rolls the transaction back upon completion.
func (o *Operation) Test(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return
	}
	defer func() {
		err = exception.Wrap(tx.Rollback())
	}()
	err = o.Invoke(c, tx)
	return
}

// Apply wraps the action in a transaction and commits it if there were no errors, rolling back if there were.
func (o *Operation) Apply(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			err = exception.Wrap(tx.Commit())
		} else {
			err = exception.WrapMany(err, exception.New(tx.Rollback()))
		}
	}()

	err = o.Invoke(c, tx)
	return
}

// Invoke runs the operation against the given connection and transaction.
func (o *Operation) Invoke(c *spiffy.DbConnection, tx *sql.Tx) error {
	return o.action(o, c, tx)
}
