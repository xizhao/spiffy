package migration

import (
	"database/sql"

	"github.com/blendlabs/spiffy"
)

// Body is an alias to NewStatement.
func Body(statements ...string) Statement {
	return NewStatement(statements...)
}

// NewStatement creates a statement block.
func NewStatement(statements ...string) Statement {
	return Statement(statements)
}

// Statement is an atomic unit of work. It can be multiple individual sql statements.
// This is what is run by the operation gates (if index exists / if column exists etc.)
type Statement []string

// Invoke executes the statement block
func (s Statement) Invoke(c *spiffy.Connection, tx *sql.Tx) (err error) {
	for _, step := range s {
		err = c.ExecInTx(step, tx)
		if err != nil {
			return
		}
	}
	return
}

// Invoke returns an invocable that can have a body.
func Invoke(action Invocation) *DynamicInvocation {
	return &DynamicInvocation{Action: action}
}

// DynamicInvocation wraps a user supplied invocation body.
type DynamicInvocation struct {
	Action Invocation
}

// Invoke applies the invocation.
func (di *DynamicInvocation) Invoke(c *spiffy.Connection, tx *sql.Tx) error {
	return di.Action(c, tx)
}
