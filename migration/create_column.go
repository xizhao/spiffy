package migration

import (
	"database/sql"

	"github.com/blendlabs/spiffy"
)

// CreateColumnIfNotExists creates a table on the given connection if it does not exist.
func CreateColumnIfNotExists(connection *spiffy.DbConnection, tx *sql.Tx, tableName, columnName, statement string) error {
	if exists, err := ColumnExists(connection, tx, tableName, columnName); err != nil {
		return err
	} else if !exists {
		return connection.ExecInTransaction(statement, tx)
	}
	return nil
}
