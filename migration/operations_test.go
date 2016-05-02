package migration

import (
	"fmt"
	"testing"

	"github.com/blendlabs/go-assert"
	"github.com/blendlabs/go-util"
	"github.com/blendlabs/spiffy"
)

func TestCreateTable(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := util.RandomString(12)
	body := fmt.Sprintf("CREATE TABLE %s (id int, name varchar(32));", tableName)

	op := Operation(CreateTable, Statement(body), tableName)
	err = op.Invoke(spiffy.DefaultDb(), tx)
	assert.Nil(err)

	exists, err := TableExists(spiffy.DefaultDb(), tx, tableName)
	assert.Nil(err)
	assert.True(exists)
}

func TestCreateColumn(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := util.RandomString(12)
	body := fmt.Sprintf("CREATE TABLE %s (id int, name varchar(32));", tableName)

	op := Operation(CreateTable, Statement(body), tableName)
	err = op.Invoke(spiffy.DefaultDb(), tx)
	assert.Nil(err)

	exists, err := TableExists(spiffy.DefaultDb(), tx, tableName)
	assert.Nil(err)
	assert.True(exists)
}
