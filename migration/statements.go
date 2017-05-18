package migration

import (
	"database/sql"

	"github.com/blendlabs/spiffy"
)

// Statements is an alias to NewStatement.
func Statements(stmts ...string) Invocable {
	return statements(stmts)
}

// BodyStatements is an atomic unit of work. It can be multiple individual sql statements.
// This is what is run by the operation gates (if index exists / if column exists etc.)
type statements []string

// Invoke executes the statement block
func (s statements) Invoke(c *spiffy.Connection, tx *sql.Tx) (err error) {
	for _, step := range s {
		err = c.ExecInTx(step, tx)
		if err != nil {
			return
		}
	}
	return
}
