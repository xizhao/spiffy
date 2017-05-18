package migration

import (
	"database/sql"

	"github.com/blendlabs/spiffy"
)

// Invoke returns an invocable that can have a body.
func Invoke(action InvocableAction) *DynamicInvocation {
	return &DynamicInvocation{Action: action}
}

// DynamicInvocation wraps a user supplied invocation body.
type DynamicInvocation struct {
	Action InvocableAction
}

// Invoke applies the invocation.
func (di *DynamicInvocation) Invoke(c *spiffy.Connection, tx *sql.Tx) error {
	return di.Action(c, tx)
}
