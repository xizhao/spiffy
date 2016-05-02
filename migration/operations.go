package migration

import (
	"database/sql"
	"strings"

	"github.com/blendlabs/spiffy"
)

// CreateColumn creates a table on the given connection if it does not exist.
func CreateColumn(c *spiffy.DbConnection, tx *sql.Tx, statement StatementBlock, args ...string) error {
	if exists, err := ColumnExists(c, tx, args[0], args[1]); err != nil {
		return err
	} else if !exists {
		return statement.Run(c, tx)
	}
	return nil
}

// CreateConstraint creates a table on the given connection if it does not exist.
func CreateConstraint(c *spiffy.DbConnection, tx *sql.Tx, statement StatementBlock, args ...string) error {
	if exists, err := ConstraintExists(c, tx, args[0]); err != nil {
		return err
	} else if !exists {
		return statement.Run(c, tx)
	}
	return nil
}

// CreateTable creates a table on the given connection if it does not exist.
func CreateTable(c *spiffy.DbConnection, tx *sql.Tx, statement StatementBlock, args ...string) error {
	if exists, err := TableExists(c, tx, args[0]); err != nil {
		return err
	} else if !exists {
		return statement.Run(c, tx)
	}
	return nil
}

// CreateIndex creates a index on the given connection if it does not exist.
func CreateIndex(c *spiffy.DbConnection, tx *sql.Tx, statement StatementBlock, args ...string) error {
	if exists, err := IndexExists(c, tx, args[0], args[1]); err != nil {
		return err
	} else if !exists {
		return statement.Run(c, tx)
	}
	return nil
}

// AlterColumn alters an existing column, erroring if it doesn't exist
func AlterColumn(c *spiffy.DbConnection, tx *sql.Tx, statement StatementBlock, args ...string) error {
	if exists, err := ColumnExists(c, tx, args[0], args[1]); err != nil {
		return err
	} else if exists {
		return statement.Run(c, tx)
	}
	return nil
}

// AlterConstraint alters an existing constraint, erroring if it doesn't exist
func AlterConstraint(c *spiffy.DbConnection, tx *sql.Tx, statement StatementBlock, args ...string) error {
	if exists, err := ConstraintExists(c, tx, args[0]); err != nil {
		return err
	} else if exists {
		return statement.Run(c, tx)
	}
	return nil
}

// AlterTable alters an existing table, erroring if it doesn't exist
func AlterTable(c *spiffy.DbConnection, tx *sql.Tx, statement StatementBlock, args ...string) error {
	if exists, err := TableExists(c, tx, args[0]); err != nil {
		return err
	} else if exists {
		return statement.Run(c, tx)
	}
	return nil
}

// AlterIndex alters an existing index, erroring if it doesn't exist
func AlterIndex(c *spiffy.DbConnection, tx *sql.Tx, statement StatementBlock, args ...string) error {
	if exists, err := IndexExists(c, tx, args[0], args[1]); err != nil {
		return err
	} else if exists {
		return statement.Run(c, tx)
	}
	return nil
}

// TableExists returns if a table exists on the given connection.
func TableExists(c *spiffy.DbConnection, tx *sql.Tx, tableName string) (bool, error) {
	return c.QueryInTransaction(`SELECT 1 FROM pg_catalog.pg_tables WHERE tablename = $1`, tx, strings.ToLower(tableName)).Any()
}

// ColumnExists returns if a column exists on a table on the given connection.
func ColumnExists(c *spiffy.DbConnection, tx *sql.Tx, tableName, columnName string) (bool, error) {
	return c.QueryInTransaction(`SELECT 1 FROM information_schema.columns i WHERE i.table_name = $1 and i.column_name = $2`, tx, strings.ToLower(tableName), strings.ToLower(columnName)).Any()
}

// ConstraintExists returns if a constraint exists on a table on the given connection.
func ConstraintExists(c *spiffy.DbConnection, tx *sql.Tx, constraintName string) (bool, error) {
	return c.QueryInTransaction(`SELECT 1 FROM pg_constraint WHERE conname = $1`, tx, strings.ToLower(constraintName)).Any()
}

// IndexExists returns if a index exists on a table on the given connection.
func IndexExists(c *spiffy.DbConnection, tx *sql.Tx, tableName, indexName string) (bool, error) {
	return c.QueryInTransaction(`SELECT 1 FROM pg_catalog.pg_index ix join pg_catalog.pg_class t on t.oid = ix.indrelid join pg_catalog.pg_class i on i.oid = ix.indexrelid WHERE t.relname = $1 and i.relname = $2 and t.relkind = 'r'`, tx, strings.ToLower(tableName), strings.ToLower(indexName)).Any()
}
