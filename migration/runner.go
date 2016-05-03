package migration

import (
	"database/sql"
	"log"

	"github.com/blendlabs/go-util"
	"github.com/blendlabs/spiffy"
)

// Series is an alias to New.
func Series(migrations ...Migration) Migration {
	return New(util.StringEmpty, migrations...)
}

// New creates a new migration series.
func New(name string, migrations ...Migration) Migration {
	return &Runner{
		Name:       name,
		Migrations: migrations,
	}
}

// Runner runs the migrations
type Runner struct {
	Name       string
	Logger     *log.Logger
	Migrations []Migration
}

// SetLogger sets the logger the Runner should use.
func (r *Runner) SetLogger(logger *log.Logger) {
	r.Logger = logger
}

// Test runs the migration suite and then rolls it back.
func (r Runner) Test(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return err
	}
	defer func() {
		err = tx.Rollback()
	}()

	setLoggerPhase(r.Logger, "test", r.Name)
	defer setLoggerPhase(r.Logger, util.StringEmpty, util.StringEmpty)

	err = r.Invoke(c, tx)
	return
}

// Apply applies the migration suite and commits it to the db.
func (r Runner) Apply(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return err
	}
	defer func() {
		err = tx.Commit()
	}()

	setLoggerPhase(r.Logger, "apply", r.Name)
	defer setLoggerPhase(r.Logger, util.StringEmpty, util.StringEmpty)

	err = r.Invoke(c, tx)
	return
}

// Invoke runs the suite against a given connection and transaction.
func (r Runner) Invoke(c *spiffy.DbConnection, tx *sql.Tx) (err error) {
	for _, m := range r.Migrations {
		if r.Logger != nil {
			m.SetLogger(r.Logger)
		}
		err = m.Invoke(c, tx)
		if err != nil {
			return err
		}
	}
	return nil
}
