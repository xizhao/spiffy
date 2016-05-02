package migration

import (
	"database/sql"
	"log"

	"github.com/blendlabs/spiffy"
)

// New Creates a new migration that runs the sub-migrations in series.
func New(migrations ...Migration) Migration {
	return Series(migrations...)
}

// Series creates a new migration series.
func Series(migrations ...Migration) Migration {
	return &Runner{
		Migrations: migrations,
	}
}

// Runner runs the migrations
type Runner struct {
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
	err = r.Invoke(c, tx)
	return
}

// Invoke runs the suite against a given connection and transaction.
func (r Runner) Invoke(c *spiffy.DbConnection, tx *sql.Tx) (err error) {
	for _, m := range r.Migrations {
		err = m.Invoke(c, tx)
		if err != nil {
			return err
		}
	}
	return nil
}
