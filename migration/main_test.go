package migration

import (
	"os"
	"testing"

	"github.com/blendlabs/spiffy"
)

// TestMain is the testing entrypoint.
func TestMain(m *testing.M) {
	config := dbConnectionFromEnvironment()
	spiffy.CreateDbAlias("main", config)
	spiffy.SetDefaultAlias("main")

	os.Exit(m.Run())
}

func dbConnectionFromEnvironment() *spiffy.DbConnection {
	dbHost := os.Getenv("DB_HOST")
	dbSchema := os.Getenv("DB_SCHEMA")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")

	if dbHost == "" {
		dbHost = "localhost"
	}

	if dbSchema == "" {
		dbSchema = "postgres"
	}

	return spiffy.NewDbConnection(dbHost, dbSchema, dbUser, dbPassword)
}
