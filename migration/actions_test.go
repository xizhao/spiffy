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
	step := Step(CreateTable, Body(body), tableName)
	return step.Apply(spiffy.DefaultDb(), tx)
}

func createTestColumn(tableName, columnName string, tx *sql.Tx) error {
	body := fmt.Sprintf("ALTER TABLE %s ADD %s varchar(32);", tableName, columnName)
	step := Step(CreateColumn, Body(body), tableName, columnName)
	return step.Apply(spiffy.DefaultDb(), tx)
}

func createTestConstraint(tableName, constraintName string, tx *sql.Tx) error {
	body := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (name);", tableName, constraintName)
	step := Step(CreateColumn, Body(body), tableName, constraintName)
	return step.Apply(spiffy.DefaultDb(), tx)
}

func createTestIndex(tableName, indexName string, tx *sql.Tx) error {
	body := fmt.Sprintf("CREATE INDEX %s ON %s (name);", indexName, tableName)
	step := Step(CreateIndex, Body(body), tableName, indexName)
	return step.Apply(spiffy.DefaultDb(), tx)
}

func createTestRole(roleName string, tx *sql.Tx) error {
	body := fmt.Sprintf("CREATE ROLE %s;", roleName)
	step := Step(CreateRole, Body(body), roleName)
	return step.Apply(spiffy.DefaultDb(), tx)
}

func TestCreateTable(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := util.RandomString(12)
	err = createTestTable(tableName, nil)
	assert.Nil(err)

	exists, err := tableExists(spiffy.DefaultDb(), nil, tableName)
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
	err = createTestColumn(tableName, columnName, tx)
	assert.Nil(err)

	exists, err := columnExists(spiffy.DefaultDb(), tx, tableName, columnName)
	assert.Nil(err)
	assert.True(exists, "column does not exist on table")
}

func TestCreateConstraint(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := util.RandomString(12)
	err = createTestTable(tableName, tx)
	assert.Nil(err)

	constraintName := fmt.Sprintf("uk_%s_%s", tableName, util.RandomString(12))
	err = createTestConstraint(tableName, constraintName, tx)
	assert.Nil(err)

	exists, err := constraintExists(spiffy.DefaultDb(), tx, constraintName)
	assert.Nil(err)
	assert.True(exists, "constraint does not exist")
}

func TestCreateIndex(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := util.RandomString(12)
	err = createTestTable(tableName, tx)
	assert.Nil(err)

	indexName := fmt.Sprintf("ix_%s_%s", tableName, util.RandomString(12))
	err = createTestIndex(tableName, indexName, tx)
	assert.Nil(err)

	exists, err := indexExists(spiffy.DefaultDb(), tx, tableName, indexName)
	assert.Nil(err)
	assert.True(exists, "constraint does not exist")
}

func TestCreateRole(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	roleName := util.RandomString(32)
	err = createTestRole(roleName, tx)
	assert.Nil(err)

	exists, err := roleExists(spiffy.DefaultDb(), tx, roleName)
	assert.Nil(err)
	assert.True(exists, "role does not exist")
}
