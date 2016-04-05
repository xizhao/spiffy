package migration

import (
	"database/sql"

	"github.com/blendlabs/spiffy"
)

// TableExists returns if a table exists on the given connection.
func TableExists(connection *spiffy.DbConnection, tx *sql.Tx, tableName string) (bool, error) {
	return connection.QueryInTransaction(`SELECT 1 FROM	pg_catalog.pg_tables WHERE tablename = $1`, tx, tableName).Any()
}

// ColumnExists returns if a column exists on a table on the given connection.
func ColumnExists(connection *spiffy.DbConnection, tx *sql.Tx, tableName, columnName string) (bool, error) {
	return connection.QueryInTransaction(`SELECT 1 FROM information_schema.columns i WHERE i.table_name = $1 and i.column_name = $2`, tx, tableName, columnName).Any()
}

// ConstraintExists returns if a constraint exists on a table on the given connection.
func ConstraintExists(connection *spiffy.DbConnection, tx *sql.Tx, constraintName string) (bool, error) {
	return connection.QueryInTransaction(`SELECT 1 FROM pg_constraint WHERE conname = $1`, tx, constraintName).Any()
}

// IndexExists returns if a index exists on a table on the given connection.
func IndexExists(connection *spiffy.DbConnection, tx *sql.Tx, tableName, indexName string) (bool, error) {
	return connection.QueryInTransaction(`
SELECT 1 FROM pg_catalog.pg_index ix join pg_catalog.pg_class t on t.oid = ix.indrelid join pg_catalog.pg_class i on i.oid = ix.indexrelid
WHERE t.relname = $1 and i.relname = $2 and t.relkind = 'r'`, tx, tableName, indexName).Any()
}
