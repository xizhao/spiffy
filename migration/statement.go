package migration

import (
	"database/sql"

	"github.com/blendlabs/spiffy"
)

// Statement creates a statement block.
func Statement(statements ...string) StatementBlock {
	return StatementBlock(statements)
}

// StatementBlock is an atomic unit of work. It can be multiple sql statements.
type StatementBlock []string

// Run executes the statement block
func (s StatementBlock) Run(c *spiffy.DbConnection, tx *sql.Tx) (err error) {
	for _, step := range s {
		err = c.ExecInTransaction(step, tx)
		if err != nil {
			return
		}
	}
	return
}
