package main

import (
	"os"

	"github.com/blendlabs/go-util"
	"github.com/blendlabs/spiffy"
	"github.com/blendlabs/spiffy/migration"
)

func main() {
	initDb()

	m := migration.New(
		"example_table",
		migration.Step(
			migration.CreateTable,
			migration.Body(
				"CREATE TABLE example_table (id int not null, name varchar(32) not null);",
				"ALTER TABLE example_table ADD CONSTRAINT pk_example_table_id PRIMARY KEY(id);",
			),
			"example_table",
		),
		migration.Step(
			migration.CreateColumn,
			migration.Body(
				"ALTER TABLE example_table ADD foo varchar(64);",
			),
			"example_table", "foo",
		),
	)
	m.SetLogger(migration.NewLogger())

	err := m.Apply(spiffy.DefaultDb())
	if err != nil {
		os.Exit(1)
	}
}

func initDb() {
	config := dbConnectionFromEnvironment()
	spiffy.CreateDbAlias("main", config)
	spiffy.SetDefaultAlias("main")
}

func dbConnectionFromEnvironment() *spiffy.DbConnection {
	dbHost := os.Getenv("DB_HOST")
	dbSchema := os.Getenv("DB_SCHEMA")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")

	if util.IsEmpty(dbHost) {
		dbHost = "localhost"
	}

	if util.IsEmpty(dbSchema) {
		dbSchema = "postgres"
	}

	return spiffy.NewDbConnection(dbHost, dbSchema, dbUser, dbPassword)
}
