package migration

import (
	"database/sql"

	"github.com/blendlabs/spiffy"
)

// Suite creates a new migration suite.
func Suite(migrations ...Process) *SuiteRunner {
	return &SuiteRunner{
		Suite: migrations,
	}
}

// SuiteRunner runs the migrations
type SuiteRunner struct {
	Suite []Process
}

// DryRun runs the migration suite and then rolls it back.
func (r SuiteRunner) DryRun(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return err
	}
	defer func() {
		err = tx.Rollback()
	}()
	err = r.runSuite(c, tx)
	return
}

// Run applies the migration suite
func (r SuiteRunner) Run(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return err
	}
	defer func() {
		err = tx.Commit()
	}()
	err = r.runSuite(c, tx)
	return
}

// Run applies the migration suite
func (r SuiteRunner) runSuite(c *spiffy.DbConnection, tx *sql.Tx) (err error) {
	for _, m := range r.Suite {
		err = m.Apply(c, tx)
		if err != nil {
			return err
		}
	}
	return nil
}
