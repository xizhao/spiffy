package migration

import (
	"database/sql"

	"github.com/blendlabs/spiffy"
)

// Process is the interface migrations must implement.
type Process interface {
	Apply(c *spiffy.DbConnection, tx *sql.Tx) error
}

// New returns a new Process for the given steps.
func New(ops ...OperationInvocation) Process {
	return &SerialProcess{
		operations: ops,
	}
}

// Operation creates a new operation.
func Operation(action OperationAction, body StatementBlock, args ...string) OperationInvocation {
	return OperationInvocation{
		Action:     action,
		Statements: body,
		Args:       args,
	}
}

// OperationAction is a step in a process
type OperationAction func(c *spiffy.DbConnection, tx *sql.Tx, statement StatementBlock, args ...string) error

// OperationInvocation is a closure for a Operation
type OperationInvocation struct {
	Action     OperationAction
	Statements StatementBlock
	Args       []string
}

// Invoke runs the operation against the given connection and transaction.
func (oi OperationInvocation) Invoke(c *spiffy.DbConnection, tx *sql.Tx) error {
	return oi.Action(c, tx, oi.Statements, oi.Args...)
}

// SerialProcess is a migration process that runs a linear set of steps.
type SerialProcess struct {
	operations []OperationInvocation
}

// Apply runs the serial process.
func (sp *SerialProcess) Apply(c *spiffy.DbConnection, tx *sql.Tx) error {
	var err error
	for _, op := range sp.operations {
		err = op.Invoke(c, tx)
		if err != nil {
			return err
		}
	}

	return nil
}
