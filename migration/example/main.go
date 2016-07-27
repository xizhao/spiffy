package main

import (
	"os"

	"github.com/blendlabs/spiffy"
	"github.com/blendlabs/spiffy/migration"
)

func main() {
	initDb()

	m := migration.New(
		"create example_table",
		migration.New(
			"creating the table",
			migration.Step(
				migration.CreateTable,
				migration.Body(
					"CREATE TABLE example_table (id int not null, name varchar(32) not null);",
					"ALTER TABLE example_table ADD CONSTRAINT pk_example_table_id PRIMARY KEY(id);",
				),
				"example_table",
			),
		),
		migration.New(
			"adding a column",
			migration.Step(
				migration.CreateColumn,
				migration.Body(
					"ALTER TABLE example_table ADD foo varchar(64);",
				),
				"example_table", "foo",
			),
		),
	)
	m.Logged(migration.NewLogger())

	err := m.Test(spiffy.DefaultDb())
	if err != nil {
		os.Exit(1)
	}
}

func initDb() {
	config := spiffy.NewDbConnectionFromEnvironment()
	spiffy.CreateDbAlias("main", config)
	spiffy.SetDefaultAlias("main")
}
