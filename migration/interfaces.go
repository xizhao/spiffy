package migration

import (
	"database/sql"
	"log"

	"github.com/blendlabs/spiffy"
)

// Action is a step in a migration.
type Action func(c *spiffy.DbConnection, tx *sql.Tx, body Invocable, args ...string) error

// Invocation is a function that can be run in the migration suite.
type Invocation func(c *spiffy.DbConnection, tx *sql.Tx) error

// Invocable is a thing that can be invoked.
type Invocable interface {
	Invoke(c *spiffy.DbConnection, tx *sql.Tx) error
}

// Loggable is a step that is loggable.
type Loggable interface {
	SetLogger(logger *log.Logger)
}

// Migration is an Invocable that can tested before running.
type Migration interface {
	Test(c *spiffy.DbConnection) error
	Apply(c *spiffy.DbConnection) error
	Invoke(c *spiffy.DbConnection, tx *sql.Tx) error
}
