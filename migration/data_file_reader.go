package migration

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"

	exception "github.com/blendlabs/go-exception"
	"github.com/blendlabs/spiffy"
	"github.com/lib/pq"
)

const (
	regexCopyExtract = `COPY (.*)? \((.*)?\)`
)

// ReadDataFile returns a new DataFileReader
func ReadDataFile(filePath string) *DataFileReader {
	return &DataFileReader{
		path:          filePath,
		copyExtractor: regexp.MustCompile(regexCopyExtract),
	}
}

// DataFileReader reads a postgres dump.
type DataFileReader struct {
	parent *Runner
	path   string
	logger *Logger

	copyExtractor *regexp.Regexp
}

// Label returns the label for the data file reader.
func (dfr *DataFileReader) Label() string {
	return fmt.Sprintf("read data file `%s`", dfr.path)
}

// Parent returns the parent for the data file reader.
func (dfr *DataFileReader) Parent() *Runner {
	return dfr.parent
}

// SetParent sets the parent for the data file reader.
func (dfr *DataFileReader) SetParent(parent *Runner) {
	dfr.parent = parent
}

// SetLogger sets the logger for the data file reader.
func (dfr *DataFileReader) SetLogger(logger *Logger) {
	dfr.logger = logger
}

// Test runs the data file reader and then rolls-back the txn.
func (dfr *DataFileReader) Test(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return
	}
	defer func() {
		err = exception.Wrap(tx.Rollback())
	}()
	err = dfr.Invoke(c, tx)
	return
}

// Apply applies the data file reader.
func (dfr *DataFileReader) Apply(c *spiffy.DbConnection) (err error) {
	tx, err := c.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			err = exception.Wrap(tx.Commit())
		} else {
			err = exception.WrapMany(err, exception.New(tx.Rollback()))
		}
	}()

	err = dfr.Invoke(c, tx)
	return
}

// Invoke consumes the data file and writes it to the db.
func (dfr *DataFileReader) Invoke(c *spiffy.DbConnection, tx *sql.Tx) (err error) {
	defer func() {
		if err != nil {
			if dfr.logger != nil {
				dfr.logger.Errorf(dfr, err)
			}
		} else {
			if dfr.logger != nil {
				dfr.logger.Applyf(dfr, "done!")
			}
		}
	}()
	var f *os.File
	if f, err = os.Open(dfr.path); err != nil {
		return
	}
	defer f.Close()

	var stmt *sql.Stmt
	scanner := bufio.NewScanner(f)
	state := 0
	for scanner.Scan() {
		line := strings.Trim(scanner.Text(), " \t\r\n")
		if len(line) == 0 {
			continue
		}
		if strings.HasPrefix(line, "--") {
			continue
		}
		switch state {
		case 0:
			if strings.HasPrefix(line, "COPY") {
				if strings.HasSuffix(line, "FROM stdin;") {
					state = 1
					stmt, err = dfr.executeCopyLine(line, c, tx)
					if err != nil {
						return
					}
					continue
				} else {
					err = fmt.Errorf("Only `stdin` from clauses supported at this time, cannot continue.")
					return
				}
			}
			if !strings.HasPrefix(line, "SET") {
				err = c.ExecInTransaction(line, tx)
				if err != nil {
					return
				}
			}
		case 1:
			if strings.HasPrefix(line, `\.`) {
				state = 0
				err = stmt.Close()
				if err != nil {
					return
				}
				continue
			}
			err = dfr.executeDataLine(line, stmt, c, tx)
			if err != nil {
				return
			}
		}
	}
	return nil
}

func (dfr *DataFileReader) executeCopyLine(line string, c *spiffy.DbConnection, tx *sql.Tx) (*sql.Stmt, error) {
	pieces := dfr.extractCopyLine(line)
	if len(pieces) < 2 {
		return nil, exception.New("Invalid `COPY ...` line, cannot continue.")
	}
	tableName := pieces[1]
	columnCSV := pieces[2]
	return tx.Prepare(pq.CopyIn(tableName, strings.Split(columnCSV, ", ")...))
}

func (dfr *DataFileReader) executeDataLine(line string, stmt *sql.Stmt, c *spiffy.DbConnection, tx *sql.Tx) error {
	pieces := dfr.extractDataLine(line)
	_, err := stmt.Exec(pieces...)
	return err
}

// regexExtractSubMatches returns sub matches for an expr because go's regexp library is weird.
func (dfr *DataFileReader) extractCopyLine(line string) []string {
	allResults := dfr.copyExtractor.FindAllStringSubmatch(line, -1)
	results := []string{}
	for _, resultSet := range allResults {
		for _, result := range resultSet {
			results = append(results, result)
		}
	}

	return results
}

func (dfr *DataFileReader) extractDataLine(line string) []interface{} {
	var values []interface{}
	var value string
	var state int
	for index, r := range line {
		switch state {
		case 0:
			if r == rune('\t') {
				continue
			}
			state = 1
			value = value + string(r)
			if index == len(line)-1 {
				values = append(values, value)
				continue
			}
		case 1:
			if r == rune('\t') {
				state = 0
				values = append(values, value)
				value = ""
				continue
			}

			value = value + string(r)

			if index == len(line)-1 {
				values = append(values, value)
				continue
			}
		}
	}
	return values
}
