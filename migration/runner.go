package migration

import (
	"database/sql"

	"github.com/blendlabs/go-exception"
	"github.com/blendlabs/spiffy"
)

// New creates a new migration series.
func New(name string, migrations ...Migration) *Runner {
	r := &Runner{
		name: name,
	}
	r.addMigrations(migrations...)
	return r
}

// Runner runs the migrations
type Runner struct {
	name               string
	parent             *Runner
	shouldAbortOnError bool
	stack              []string
	logger             *Logger
	migrations         []Migration
}

func (r *Runner) addMigrations(migrations ...Migration) {
	for _, m := range migrations {
		m.SetParent(r)
		r.migrations = append(r.migrations, m)
	}
}

// IsRoot denotes if the runner is the root runner (or not).
func (r *Runner) IsRoot() bool {
	return r.parent == nil
}

// ShouldAbortOnError indicates that the runner will abort if it sees an error from a step.
func (r *Runner) ShouldAbortOnError() bool {
	return r.shouldAbortOnError
}

// SetShouldAbortOnError sets if the runner should abort on error.
func (r *Runner) SetShouldAbortOnError(value bool) {
	r.shouldAbortOnError = value
}

// SetLogger sets the logger the Runner should use.
func (r *Runner) SetLogger(logger *Logger) {
	r.logger = logger
}

// Test wraps the action in a transaction and rolls the transaction back upon completion.
func (r Runner) Test(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return err
	}
	defer func() {
		err = exception.Wrap(tx.Rollback())
	}()
	if r.logger != nil {
		r.logger.Phase = "test"
	}
	err = r.Invoke(c, tx)
	return
}

// Apply wraps the action in a transaction and commits it if there were no errors, rolling back if there were.
func (r Runner) Apply(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = exception.Wrap(tx.Commit())
		} else {
			err = exception.WrapMany(err, exception.New(tx.Rollback()))
		}
	}()
	if r.logger != nil {
		r.logger.Phase = "apply"
	}
	err = r.Invoke(c, tx)
	return
}

// Invoke runs the suite against a given connection and transaction.
func (r Runner) Invoke(c *spiffy.DbConnection, tx *sql.Tx) (err error) {
	for _, m := range r.migrations {
		if r.logger != nil {
			m.SetLogger(r.logger)
		}
		err = m.Invoke(c, tx)
		if err != nil && r.shouldAbortOnError {
			break
		}
	}

	if r.IsRoot() && r.logger != nil {
		r.logger.WriteStats()
	}
	return
}
