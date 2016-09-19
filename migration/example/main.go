package main

import (
	"log"

	"github.com/blendlabs/spiffy"
	"github.com/blendlabs/spiffy/migration"
)

func main() {
	err := spiffy.SetDefaultDb(spiffy.NewDbConnectionFromEnvironment())
	if err != nil {
		log.Fatal(err)
	}

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
			migration.Step(
				migration.AlwaysRun,
				migration.Body(
					`INSERT INTO example_table (id, name) select 1, 'foo' where not exists ( select 1 from example_table where id = 1)`,
				),
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
		migration.New(
			"drop the table",
			migration.Step(
				migration.AlterTable,
				migration.Body(
					"DROP TABLE example_table;",
				),
				"example_table",
			),
		),
	)
	m.SetLogger(migration.NewLogger())

	err = m.Test(spiffy.DefaultDb())
	if err != nil {
		log.Fatal(err)
	}
}
