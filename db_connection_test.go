package spiffy

import (
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/blendlabs/go-assert"
)

func TestNewAunauthenticatedDbConnection(t *testing.T) {
	a := assert.New(t)
	conn := NewDbConnection("test_host", "test_database")
	a.Equal("test_host", conn.Host)
	a.Equal("test_database", conn.Database)
	a.Equal("postgres://test_host/test_database?sslmode=disable", conn.CreatePostgresConnectionString())
}

func TestNewDbConnection(t *testing.T) {
	a := assert.New(t)
	conn := NewDbConnectionWithPassword("test_host", "test_database", "test_user", "test_password")
	a.Equal("test_host", conn.Host)
	a.Equal("test_database", conn.Database)
	a.Equal("test_user", conn.Username)
	a.Equal("test_password", conn.Password)

	a.Equal("postgres://test_user:test_password@test_host/test_database?sslmode=disable", conn.CreatePostgresConnectionString())
}

func TestNewSSLDbConnection(t *testing.T) {
	a := assert.New(t)
	conn := NewDbConnectionWithSSLMode("test_host", "test_database", "test_user", "test_password", "a good one")
	a.Equal("test_host", conn.Host)
	a.Equal("test_database", conn.Database)
	a.Equal("test_user", conn.Username)
	a.Equal("test_password", conn.Password)
	a.Equal("a good one", conn.SSLMode)
	a.Equal(`postgres://test_user:test_password@test_host/test_database?sslmode=a+good+one`, conn.CreatePostgresConnectionString())
}

// TestConnectionSanityCheck tests if we can connect to the db, a.k.a., if the underlying driver works.
func TestConnectionSanityCheck(t *testing.T) {
	config := NewDbConnectionFromEnvironment()
	_, dbErr := sql.Open("postgres", config.CreatePostgresConnectionString())
	if dbErr != nil {
		t.Error("Error opening database")
		t.FailNow()
	}
}

func TestPrepare(t *testing.T) {
	a := assert.New(t)
	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	err = createTable(tx)
	a.Nil(err)
}

func TestQuery(t *testing.T) {
	t.Skip()

	a := assert.New(t)
	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	err = seedObjects(100, tx)
	a.Nil(err)

	objs := []benchObj{}
	err = DefaultDb().QueryInTransaction("select * from bench_object", tx).OutMany(&objs)

	a.Nil(err)
	a.NotEmpty(objs)

	all := []benchObj{}
	err = DefaultDb().GetAllInTransaction(&all, tx)
	a.Nil(err)
	a.Equal(len(objs), len(all))

	obj := benchObj{}
	err = DefaultDb().QueryInTransaction("select * from bench_object limit 1", tx).Out(&obj)
	a.Nil(err)
	a.NotEqual(obj.ID, 0)

	var id int
	err = DefaultDb().QueryInTransaction("select id from bench_object limit 1", tx).Scan(&id)
	a.Nil(err)
	a.NotEqual(id, 0)
}

func TestCRUDMethods(t *testing.T) {
	a := assert.New(t)
	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	seedErr := seedObjects(100, tx)
	a.Nil(seedErr)

	objs := []benchObj{}
	queryErr := DefaultDb().QueryInTransaction("select * from bench_object", tx).OutMany(&objs)

	a.Nil(queryErr)
	a.NotEmpty(objs)

	all := []benchObj{}
	allErr := DefaultDb().GetAllInTransaction(&all, tx)
	a.Nil(allErr)
	a.Equal(len(objs), len(all))

	sampleObj := all[0]

	getTest := benchObj{}
	getTestErr := DefaultDb().GetByIDInTransaction(&getTest, tx, sampleObj.ID)
	a.Nil(getTestErr)
	a.Equal(sampleObj.ID, getTest.ID)

	exists, existsErr := DefaultDb().ExistsInTransaction(&getTest, tx)
	a.Nil(existsErr)
	a.True(exists)

	getTest.Name = "not_a_test_object"

	updateErr := DefaultDb().UpdateInTransaction(&getTest, tx)
	a.Nil(updateErr)

	verify := benchObj{}
	verifyErr := DefaultDb().GetByIDInTransaction(&verify, tx, getTest.ID)
	a.Nil(verifyErr)
	a.Equal(getTest.Name, verify.Name)

	deleteErr := DefaultDb().DeleteInTransaction(&verify, tx)
	a.Nil(deleteErr)

	delVerify := benchObj{}
	delVerifyErr := DefaultDb().GetByIDInTransaction(&delVerify, tx, getTest.ID)
	a.Nil(delVerifyErr)
}

func TestDbConnectionOpen(t *testing.T) {
	a := assert.New(t)

	testAlias := NewDbConnectionFromEnvironment()
	db, dbErr := testAlias.Open()
	a.Nil(dbErr)
	a.NotNil(db)
	defer db.Close()
}

func TestExec(t *testing.T) {
	a := assert.New(t)
	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	err = DefaultDb().ExecInTransaction("select 'ok!'", tx)
	a.Nil(err)
}

func TestIsolateToTransaction(t *testing.T) {
	a := assert.New(t)

	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	DefaultDb().IsolateToTransaction(tx)
	defer DefaultDb().ReleaseIsolation()
	a.NotNil(DefaultDb().Tx)
	a.True(DefaultDb().IsIsolatedToTransaction())
}

func TestReleaseIsolation(t *testing.T) {
	a := assert.New(t)

	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	DefaultDb().IsolateToTransaction(tx)
	defer DefaultDb().ReleaseIsolation() //this has to happen regardless (panics etc.)

	a.NotNil(DefaultDb().Tx)
	a.True(DefaultDb().IsIsolatedToTransaction())

	DefaultDb().ReleaseIsolation()
	a.Nil(DefaultDb().Tx)
	a.False(DefaultDb().IsIsolatedToTransaction())
}

func TestBeginReturnsIsolatedTransaction(t *testing.T) {
	a := assert.New(t)

	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	DefaultDb().IsolateToTransaction(tx)
	defer DefaultDb().ReleaseIsolation()

	currentTx, err := DefaultDb().Begin()
	a.Nil(err)
	a.Equal(tx, currentTx)
}

func TestDbConnectionExecuteListeners(t *testing.T) {
	a := assert.New(t)
	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	a.StartTimeout(time.Second)
	defer a.EndTimeout()

	oldListeners := DefaultDb().executeListeners
	DefaultDb().executeListeners = nil
	defer func() {
		DefaultDb().executeListeners = oldListeners
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	DefaultDb().AddExecuteListener(func(dbe *DbEvent) {
		defer wg.Done()
		a.Equal("select 'ok!'", dbe.Query)
		a.NotZero(dbe.Elapsed)
	})

	err = DefaultDb().ExecInTransaction("select 'ok!'", tx)
	a.Nil(err)
}

func TestDbConnectionQueryListeners(t *testing.T) {
	a := assert.New(t)
	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	a.StartTimeout(time.Second)
	defer a.EndTimeout()

	oldListeners := DefaultDb().queryListeners
	DefaultDb().queryListeners = nil
	defer func() {
		DefaultDb().queryListeners = oldListeners
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	DefaultDb().AddQueryListener(func(dbe *DbEvent) {
		defer wg.Done()
		a.Equal("select 'ok!'", dbe.Query)
		a.NotZero(dbe.Elapsed)
	})

	var result string
	err = DefaultDb().QueryInTransaction("select 'ok!'", tx).Scan(&result)
	a.Nil(err)
	a.Equal("ok!", result)
}
