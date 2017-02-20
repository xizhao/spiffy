package main

import (
	"log"

	"github.com/blendlabs/spiffy"
	"github.com/blendlabs/spiffy/migration"
)

func main() {
	err := spiffy.InitDefault(spiffy.NewConnectionFromEnvironment())
	if err != nil {
		log.Fatal(err)
	}

	m := migration.New(
		"create & fill `test_vocab`",
		migration.New(
			"create `test_vocab` table",
			migration.Step(
				migration.CreateTable,
				migration.Body(
					"CREATE TABLE test_vocab (id serial not null, word varchar(32) not null);",
					"ALTER TABLE test_vocab ADD CONSTRAINT pk_test_vocab_id PRIMARY KEY(id);",
				),
				"test_vocab",
			),
		),
		migration.New(
			"fill `test_vocab`",
			migration.ReadDataFile("data.sql"),
		),
		migration.New(
			"drop `test_vocab` table",
			migration.Step(
				migration.AlterTable,
				migration.Body(
					"DROP TABLE test_vocab;",
				),
				"test_vocab",
			),
		),
	)
	m.SetLogger(migration.NewLogger())
	m.SetShouldAbortOnError(true)
	err = m.Apply(spiffy.DefaultDb())
	if err != nil {
		log.Fatal(err)
	}
}
