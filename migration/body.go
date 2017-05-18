package migration

import (
	"database/sql"

	"github.com/blendlabs/spiffy"
)

// Body returns a dynamic body invocable.
func Body(action InvocableAction) Invocable {
	return &body{Action: action}
}

// body wraps a user supplied invocation body.
type body struct {
	Action InvocableAction
}

// Invoke applies the invocation.
func (b *body) Invoke(c *spiffy.Connection, tx *sql.Tx) error {
	return b.Action(c, tx)
}
