package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	util "github.com/blendlabs/go-util"
	"github.com/blendlabs/spiffy"
	"github.com/blendlabs/spiffy/migration"
)

const (
	itemCount   = 1 << 12
	queryCount  = 1 << 10
	threadCount = 64
)

const (
	selectQuery = `SELECT * FROM test_object LIMIT $1`
)

func newTestObject() *testObject {
	return &testObject{
		UUID:       spiffy.UUIDv4().ToShortString(),
		CreatedUTC: time.Now().UTC(),
		Active:     true,
		Name:       spiffy.UUIDv4().ToShortString(),
		Variance:   rand.Float64(),
	}
}

type testObject struct {
	ID         int        `db:"id,pk,serial"`
	UUID       string     `db:"uuid"`
	CreatedUTC time.Time  `db:"created_utc"`
	UpdatedUTC *time.Time `db:"updated_utc"`
	Active     bool       `db:"active"`
	Name       string     `db:"name"`
	Variance   float64    `db:"variance"`
}

func (to *testObject) PopulateInternal(rows *sql.Rows) error {
	return rows.Scan(&to.ID, &to.UUID, &to.CreatedUTC, &to.UpdatedUTC, &to.Active, &to.Name, &to.Variance)
}

func (to testObject) TableName() string {
	return "test_object"
}

func createTable() error {
	m := migration.New(
		"create `test_object` table",
		migration.Step(
			migration.AlterTable,
			migration.Body(
				`DROP TABLE IF EXISTS test_object`,
			),
			"test_object",
		),
		migration.Step(
			migration.CreateTable,
			migration.Body("CREATE TABLE test_object (id serial not null, uuid varchar(64) not null, created_utc timestamp not null, updated_utc timestamp, active boolean, name varchar(64), variance float)"),
			"test_object",
		),
	)
	return m.Apply(spiffy.DefaultDb())
}

func seedObjects(count int) error {
	var err error
	for x := 0; x < count; x++ {
		err = spiffy.DefaultDb().Create(newTestObject())
		if err != nil {
			return err
		}
	}
	return nil
}

func baselineAccess(db *sql.DB, queryLimit int) ([]testObject, error) {
	var results []testObject
	res, err := db.Query(selectQuery, queryLimit)
	if err != nil {
		return results, err
	}

	for res.Next() {
		to := newTestObject()
		err = to.PopulateInternal(res)
		if err != nil {
			return results, err
		}
		results = append(results, *to)
	}

	return results, nil
}

func spiffyAccess(_ *sql.DB, queryLimit int) ([]testObject, error) {
	var results []testObject
	err := spiffy.DefaultDb().Query(selectQuery, queryLimit).OutMany(&results)
	return results, err
}

func benchHarness(parallelism int, queryLimit int, accessFunc func(*sql.DB, int) ([]testObject, error)) ([]time.Duration, error) {
	var durations []time.Duration
	var waitHandle = sync.WaitGroup{}
	var errors = make(chan error, parallelism)

	waitHandle.Add(parallelism)
	for threadID := 0; threadID < parallelism; threadID++ {
		go func() {
			defer waitHandle.Done()
			start := time.Now()
			items, err := accessFunc(spiffy.DefaultDb().Connection, queryLimit)
			if err != nil {
				errors <- err
				return
			}

			if len(items) < queryLimit {
				errors <- fmt.Errorf("Returned item count less than %d", queryLimit)
				return
			}

			if len(items[len(items)>>1].UUID) == 0 {
				errors <- fmt.Errorf("Returned items have empty `UUID` fields")
				return
			}

			if len(items[len(items)>>1].Name) == 0 {
				errors <- fmt.Errorf("Returned items have empty `Name` fields")
				return
			}

			if items[len(items)>>1].Variance == 0 {
				errors <- fmt.Errorf("Returned items have empty `Variance`")
				return
			}

			durations = append(durations, time.Since(start))
		}()
	}
	waitHandle.Wait()

	if len(errors) > 0 {
		return durations, <-errors
	}
	return durations, nil
}

func main() {
	err := spiffy.SetDefaultDb(spiffy.NewDbConnectionFromEnvironment())
	if err != nil {
		log.Fatal(err)
	}

	err = createTable()
	if err != nil {
		log.Fatal(err)
	}

	err = seedObjects(itemCount)
	if err != nil {
		log.Fatal(err)
	}

	// do baseline query
	baselineTimings, err := benchHarness(threadCount, queryCount, baselineAccess)
	if err != nil {
		log.Fatal(err)
	}

	// do spiffy query
	spiffyTimings, err := benchHarness(threadCount, queryCount, spiffyAccess)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Timings:")
	fmt.Printf("\tAvg Baseline  : %v\n", util.Math.MeanOfDuration(baselineTimings))
	fmt.Printf("\tAvg Spiffy    : %v\n", util.Math.MeanOfDuration(spiffyTimings))

	fmt.Printf("\t99th Baseline : %v\n", util.Math.PercentileOfDuration(baselineTimings, 99.0))
	fmt.Printf("\t99th Spiffy   : %v\n", util.Math.PercentileOfDuration(spiffyTimings, 99.0))
}
