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
	createCount    = 1 << 12
	selectCount    = 512
	iterationCount = 64
	threadCount    = 128
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

func baselineAccess(db *spiffy.DbConnection, queryLimit int) ([]testObject, error) {
	var results []testObject
	var err error

	stmt, err := db.Connection.Prepare(selectQuery)
	if err != nil {
		return results, err
	}

	res, err := stmt.Query(queryLimit)
	if err != nil {
		return results, err
	}

	if res.Err() != nil {
		return results, res.Err()
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

func spiffyAccess(db *spiffy.DbConnection, queryLimit int) ([]testObject, error) {
	var results []testObject
	err := db.Query(selectQuery, queryLimit).OutMany(&results)
	return results, err
}

func benchHarness(db *spiffy.DbConnection, parallelism int, queryLimit int, accessFunc func(*spiffy.DbConnection, int) ([]testObject, error)) ([]time.Duration, error) {
	var durations []time.Duration
	var waitHandle = sync.WaitGroup{}
	var errors = make(chan error, parallelism)

	waitHandle.Add(parallelism)
	for threadID := 0; threadID < parallelism; threadID++ {
		go func() {
			defer waitHandle.Done()

			for iteration := 0; iteration < iterationCount; iteration++ {
				start := time.Now()
				items, err := accessFunc(db, queryLimit)
				if err != nil {
					errors <- err
					return
				}

				durations = append(durations, time.Since(start))

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
			}
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

	err = seedObjects(createCount)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Finished seeding objects, starting load test.")

	// do spiffy query
	uncached := spiffy.NewDbConnectionFromEnvironment()
	uncached.DontUseStatementCache()
	_, err = uncached.Open()
	if err != nil {
		log.Fatal(err)
	}

	spiffyStart := time.Now()
	spiffyTimings, err := benchHarness(uncached, threadCount, selectCount, spiffyAccess)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Spiffy Elapsed: %v\n", time.Since(spiffyStart))

	// do spiffy query
	cached := spiffy.NewDbConnectionFromEnvironment()
	cached.UseStatementCache()
	_, err = cached.Open()
	if err != nil {
		log.Fatal(err)
	}

	spiffyCachedStart := time.Now()
	spiffyCachedTimings, err := benchHarness(cached, threadCount, selectCount, spiffyAccess)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Spiffy (Statement Cache) Elapsed: %v\n", time.Since(spiffyCachedStart))

	// do baseline query
	baselineStart := time.Now()
	baseline := spiffy.NewDbConnectionFromEnvironment()
	_, err = baseline.Open()
	if err != nil {
		log.Fatal(err)
	}

	baselineTimings, err := benchHarness(baseline, threadCount, selectCount, baselineAccess)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Baseline Elapsed: %v\n", time.Since(baselineStart))

	println()

	fmt.Println("Timings Aggregates:")
	fmt.Printf("\tAvg Baseline                 : %v\n", util.Math.MeanOfDuration(baselineTimings))
	fmt.Printf("\tAvg Spiffy                   : %v\n", util.Math.MeanOfDuration(spiffyTimings))
	fmt.Printf("\tAvg Spiffy (Statement Cache) : %v\n", util.Math.MeanOfDuration(spiffyCachedTimings))

	println()

	fmt.Printf("\t99th Baseline                 : %v\n", util.Math.PercentileOfDuration(baselineTimings, 99.0))
	fmt.Printf("\t99th Spiffy                   : %v\n", util.Math.PercentileOfDuration(spiffyTimings, 99.0))
	fmt.Printf("\t99th Spiffy (Statement Cache) : %v\n", util.Math.PercentileOfDuration(spiffyCachedTimings, 99.0))
}
