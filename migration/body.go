package migration

import (
	"database/sql"

	"github.com/blendlabs/spiffy"
)

// Body is an alias to NewStatement.
func Body(statements ...string) BodyStatement {
	return BodyStatement(statements)
}

// BodyStatement is an atomic unit of work. It can be multiple individual sql statements.
// This is what is run by the operation gates (if index exists / if column exists etc.)
type BodyStatement []string

// Invoke executes the statement block
func (bs BodyStatement) Invoke(c *spiffy.Connection, tx *sql.Tx) (err error) {
	for _, step := range bs {
		err = c.ExecInTx(step, tx)
		if err != nil {
			return
		}
	}
	return
}
