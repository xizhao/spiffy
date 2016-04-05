package migration

import (
	"database/sql"

	"github.com/blendlabs/spiffy"
)

// CreateTableIfNotExists creates a table on the given connection if it does not exist.
func CreateTableIfNotExists(connection *spiffy.DbConnection, tx *sql.Tx, tableName, statement string) error {
	if exists, err := TableExists(connection, tx, tableName); err != nil {
		return err
	} else if !exists {
		return connection.ExecInTransaction(statement, tx)
	}
	return nil
}
