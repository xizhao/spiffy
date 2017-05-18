package main

import (
	"log"

	"database/sql"

	"github.com/blendlabs/spiffy"
	"github.com/blendlabs/spiffy/migration"
)

func main() {
	err := spiffy.OpenDefault(spiffy.NewConnectionFromEnvironment())
	if err != nil {
		log.Fatal(err)
	}

	m := migration.New(
		"create & fill `test_vocab`",
		migration.Step(
			migration.AlterTable,
			migration.Body(
				"DROP TABLE test_vocab",
			),
			"test_vocab",
		),
		migration.Step(
			migration.CreateTable,
			migration.Body(
				"CREATE TABLE test_vocab (id serial not null, word varchar(32) not null);",
				"ALTER TABLE test_vocab ADD CONSTRAINT pk_test_vocab_id PRIMARY KEY(id);",
			),
			"test_vocab",
		),
		migration.New(
			"fill `test_vocab`",
			migration.ReadDataFile("data.sql"),
		),
		migration.Step(
			migration.Custom("test custom step", func(c *spiffy.Connection, tx *sql.Tx) (bool, error) {
				return c.QueryInTx("select 1 from test_vocab where word = $1", tx, "foo").None()
			}),
			migration.Invoke(func(c *spiffy.Connection, tx *sql.Tx) error {
				return c.ExecInTx("insert into test_vocab (word) values ($1)", tx, "foo")
			}),
		),
		migration.Step(
			migration.AlterTable,
			migration.Body(
				"DROP TABLE test_vocab",
			),
			"test_vocab",
		),
	)
	m.SetLogger(migration.NewLogger())
	m.SetShouldAbortOnError(true)
	err = m.Apply(spiffy.Default())
	if err != nil {
		log.Fatal(err)
	}
}
