package migration

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/blendlabs/go-assert"
	"github.com/blendlabs/go-util"
	"github.com/blendlabs/spiffy"
)

func createTestTable(tableName string, tx *sql.Tx) error {
	body := fmt.Sprintf("CREATE TABLE %s (id int, name varchar(32));", tableName)
	op := Op(CreateTable, Body(body), tableName)
	return op.Invoke(spiffy.DefaultDb(), tx)
}

func createTestColumnTable(tableName, columnName string, tx *sql.Tx) error {
	body := fmt.Sprintf("ALTER TABLE %s ADD %s varchar(32);", tableName, columnName)
	op := Op(CreateColumn, Body(body), tableName, columnName)
	return op.Invoke(spiffy.DefaultDb(), tx)
}

func TestCreateTable(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := util.RandomString(12)
	err = createTestTable(tableName, nil)
	assert.Nil(err)

	exists, err := TableExists(spiffy.DefaultDb(), nil, tableName)
	assert.Nil(err)
	assert.True(exists, "table does not exist")
}

func TestCreateColumn(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := util.RandomString(12)
	err = createTestTable(tableName, tx)
	assert.Nil(err)

	columnName := util.RandomString(12)
	err = createTestColumnTable(tableName, columnName, tx)
	assert.Nil(err)

	exists, err := ColumnExists(spiffy.DefaultDb(), tx, tableName, columnName)
	assert.Nil(err)
	assert.True(exists, "column does not exist on table")
}
