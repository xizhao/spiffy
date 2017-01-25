package spiffy

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sync"

	"github.com/blendlabs/go-assert"
)

func TestNewAunauthenticatedDbConnection(t *testing.T) {
	a := assert.New(t)
	conn := NewDbConnectionWithHost("test_host", "test_database")
	a.Equal("test_host", conn.Host)
	a.Equal("test_database", conn.Database)
	str, err := conn.CreatePostgresConnectionString()
	a.Nil(err)
	a.Equal("postgres://test_host/test_database?sslmode=disable", str)
}

func TestNewDbConnection(t *testing.T) {
	a := assert.New(t)
	conn := NewDbConnectionWithPassword("test_host", "test_database", "test_user", "test_password")
	a.Equal("test_host", conn.Host)
	a.Equal("test_database", conn.Database)
	a.Equal("test_user", conn.Username)
	a.Equal("test_password", conn.Password)
	str, err := conn.CreatePostgresConnectionString()
	a.Nil(err)
	a.Equal("postgres://test_user:test_password@test_host/test_database?sslmode=disable", str)
}

func TestNewSSLDbConnection(t *testing.T) {
	a := assert.New(t)
	conn := NewDbConnectionWithSSLMode("test_host", "test_database", "test_user", "test_password", "a good one")
	a.Equal("test_host", conn.Host)
	a.Equal("test_database", conn.Database)
	a.Equal("test_user", conn.Username)
	a.Equal("test_password", conn.Password)
	a.Equal("a good one", conn.SSLMode)
	str, err := conn.CreatePostgresConnectionString()
	a.Nil(err)
	a.Equal(`postgres://test_user:test_password@test_host/test_database?sslmode=a+good+one`, str)
}

// TestConnectionSanityCheck tests if we can connect to the db, a.k.a., if the underlying driver works.
func TestConnectionSanityCheck(t *testing.T) {
	assert := assert.New(t)
	config := NewDbConnectionFromEnvironment()
	str, err := config.CreatePostgresConnectionString()
	assert.Nil(err)
	_, err = sql.Open("postgres", str)
	assert.Nil(err)
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
	err = DefaultDb().QueryInTx("select * from bench_object", tx).OutMany(&objs)

	a.Nil(err)
	a.NotEmpty(objs)

	all := []benchObj{}
	err = DefaultDb().GetAllInTx(&all, tx)
	a.Nil(err)
	a.Equal(len(objs), len(all))

	obj := benchObj{}
	err = DefaultDb().QueryInTx("select * from bench_object limit 1", tx).Out(&obj)
	a.Nil(err)
	a.NotEqual(obj.ID, 0)

	var id int
	err = DefaultDb().QueryInTx("select id from bench_object limit 1", tx).Scan(&id)
	a.Nil(err)
	a.NotEqual(id, 0)
}

func TestDbConnectionStatementCacheExecute(t *testing.T) {
	a := assert.New(t)

	conn := NewDbConnectionFromEnvironment()
	defer func() {
		closeErr := conn.Close()
		a.Nil(closeErr)
	}()

	conn.EnableStatementCache()
	_, err := conn.Open()
	a.Nil(err)

	err = conn.Exec("select 'ok!'")
	a.Nil(err)

	err = conn.Exec("select 'ok!'")
	a.Nil(err)

	a.True(conn.StatementCache().HasStatement("select 'ok!'"))
}

func TestDbConnectionStatementCacheQuery(t *testing.T) {
	a := assert.New(t)

	conn := NewDbConnectionFromEnvironment()
	defer func() {
		closeErr := conn.Close()
		a.Nil(closeErr)
	}()

	conn.EnableStatementCache()
	_, err := conn.Open()
	a.Nil(err)

	var ok string
	err = conn.Query("select 'ok!'").Scan(&ok)
	a.Nil(err)
	a.Equal("ok!", ok)

	err = conn.Query("select 'ok!'").Scan(&ok)
	a.Nil(err)
	a.Equal("ok!", ok)

	a.True(conn.StatementCache().HasStatement("select 'ok!'"))
}

func TestCRUDMethods(t *testing.T) {
	a := assert.New(t)
	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	seedErr := seedObjects(100, tx)
	a.Nil(seedErr)

	objs := []benchObj{}
	queryErr := DefaultDb().QueryInTx("select * from bench_object", tx).OutMany(&objs)

	a.Nil(queryErr)
	a.NotEmpty(objs)

	all := []benchObj{}
	allErr := DefaultDb().GetAllInTx(&all, tx)
	a.Nil(allErr)
	a.Equal(len(objs), len(all))

	sampleObj := all[0]

	getTest := benchObj{}
	getTestErr := DefaultDb().GetByIDInTx(&getTest, tx, sampleObj.ID)
	a.Nil(getTestErr)
	a.Equal(sampleObj.ID, getTest.ID)

	exists, existsErr := DefaultDb().ExistsInTx(&getTest, tx)
	a.Nil(existsErr)
	a.True(exists)

	getTest.Name = "not_a_test_object"

	updateErr := DefaultDb().UpdateInTx(&getTest, tx)
	a.Nil(updateErr)

	verify := benchObj{}
	verifyErr := DefaultDb().GetByIDInTx(&verify, tx, getTest.ID)
	a.Nil(verifyErr)
	a.Equal(getTest.Name, verify.Name)

	deleteErr := DefaultDb().DeleteInTx(&verify, tx)
	a.Nil(deleteErr)

	delVerify := benchObj{}
	delVerifyErr := DefaultDb().GetByIDInTx(&delVerify, tx, getTest.ID)
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

	err = DefaultDb().ExecInTx("select 'ok!'", tx)
	a.Nil(err)
}

func TestIsolateToTransaction(t *testing.T) {
	a := assert.New(t)

	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	DefaultDb().IsolateToTransaction(tx)
	defer DefaultDb().ReleaseIsolation()
	a.NotNil(DefaultDb().tx)
	a.True(DefaultDb().IsIsolatedToTransaction())
}

func TestReleaseIsolation(t *testing.T) {
	a := assert.New(t)

	tx, err := DefaultDb().Begin()
	a.Nil(err)
	defer tx.Rollback()

	DefaultDb().IsolateToTransaction(tx)
	defer DefaultDb().ReleaseIsolation() //this has to happen regardless (panics etc.)

	a.NotNil(DefaultDb().tx)
	a.True(DefaultDb().IsIsolatedToTransaction())

	DefaultDb().ReleaseIsolation()
	a.Nil(DefaultDb().tx)
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

func TestDbConnectionCreate(t *testing.T) {
	assert := assert.New(t)
	tx, err := DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	err = createTable(tx)
	assert.Nil(err)

	obj := &benchObj{
		Name:      fmt.Sprintf("test_object_0"),
		Timestamp: time.Now().UTC(),
		Amount:    1000.0 + (5.0 * float32(0)),
		Pending:   true,
		Category:  fmt.Sprintf("category_%d", 0),
	}
	err = DefaultDb().CreateInTx(obj, tx)
	assert.Nil(err)
}

func TestDbConnectionCreateParallel(t *testing.T) {
	assert := assert.New(t)

	err := createTable(nil)
	assert.Nil(err)
	defer dropTable(nil)

	wg := sync.WaitGroup{}
	wg.Add(5)
	for x := 0; x < 5; x++ {
		go func() {
			defer wg.Done()
			obj := &benchObj{
				Name:      fmt.Sprintf("test_object_0"),
				Timestamp: time.Now().UTC(),
				Amount:    1000.0 + (5.0 * float32(0)),
				Pending:   true,
				Category:  fmt.Sprintf("category_%d", 0),
			}
			innerErr := DefaultDb().CreateInTx(obj, nil)
			assert.Nil(innerErr)
		}()
	}
	wg.Wait()
}

func TestDbConnectionUpsert(t *testing.T) {
	assert := assert.New(t)
	tx, err := DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	err = createUpserObjectTable(tx)
	assert.Nil(err)

	obj := &upsertObj{
		UUID:      UUIDv4().ToShortString(),
		Timestamp: time.Now().UTC(),
		Category:  UUIDv4().ToShortString(),
	}
	err = DefaultDb().UpsertInTx(obj, tx)
	assert.Nil(err)

	var verify upsertObj
	err = DefaultDb().GetByIDInTx(&verify, tx, obj.UUID)
	assert.Nil(err)
	assert.Equal(obj.Category, verify.Category)

	obj.Category = "test"

	err = DefaultDb().UpsertInTx(obj, tx)
	assert.Nil(err)

	err = DefaultDb().GetByIDInTx(&verify, tx, obj.UUID)
	assert.Nil(err)
	assert.Equal(obj.Category, verify.Category)
}

func TestDbConnectionUpsertWithSerial(t *testing.T) {
	assert := assert.New(t)
	tx, err := DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	err = createTable(tx)
	assert.Nil(err)

	obj := &benchObj{
		Name:      "test_object_0",
		Timestamp: time.Now().UTC(),
		Amount:    1005.0,
		Pending:   true,
		Category:  "category_0",
	}
	err = DefaultDb().UpsertInTx(obj, tx)
	assert.Nil(err)
	assert.NotZero(obj.ID)

	var verify benchObj
	err = DefaultDb().GetByIDInTx(&verify, tx, obj.ID)
	assert.Nil(err)
	assert.Equal(obj.Category, verify.Category)

	obj.Category = "test"

	err = DefaultDb().UpsertInTx(obj, tx)
	assert.Nil(err)
	assert.NotZero(obj.ID)

	err = DefaultDb().GetByIDInTx(&verify, tx, obj.ID)
	assert.Nil(err)
	assert.Equal(obj.Category, verify.Category)
}

func TestDbConnectionCreateMany(t *testing.T) {
	assert := assert.New(t)
	tx, err := DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	err = createTable(tx)
	assert.Nil(err)

	var objects []DatabaseMapped
	for x := 0; x < 10; x++ {
		objects = append(objects, benchObj{
			Name:      fmt.Sprintf("test_object_%d", x),
			Timestamp: time.Now().UTC(),
			Amount:    1005.0,
			Pending:   true,
			Category:  fmt.Sprintf("category_%d", x),
		})
	}

	err = DefaultDb().CreateManyInTx(objects, tx)
	assert.Nil(err)

	var verify []benchObj
	err = DefaultDb().QueryInTx(`select * from bench_object`, tx).OutMany(&verify)
	assert.Nil(err)
	assert.NotEmpty(verify)
}

func TestDbConnectionCreateIfNotExists(t *testing.T) {
	assert := assert.New(t)
	tx, err := DefaultDb().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	err = createUpserObjectTable(tx)
	assert.Nil(err)

	obj := &upsertObj{
		UUID:      UUIDv4().ToShortString(),
		Timestamp: time.Now().UTC(),
		Category:  UUIDv4().ToShortString(),
	}
	err = DefaultDb().CreateIfNotExistsInTx(obj, tx)
	assert.Nil(err)

	var verify upsertObj
	err = DefaultDb().GetByIDInTx(&verify, tx, obj.UUID)
	assert.Nil(err)
	assert.Equal(obj.Category, verify.Category)

	oldCategory := obj.Category
	obj.Category = "test"

	err = DefaultDb().CreateIfNotExistsInTx(obj, tx)
	assert.Nil(err)

	err = DefaultDb().GetByIDInTx(&verify, tx, obj.UUID)
	assert.Nil(err)
	assert.Equal(oldCategory, verify.Category)
}

func TestDbConnectionInvalidatesBadCachedStatements(t *testing.T) {
	assert := assert.New(t)

	conn := NewDbConnectionFromEnvironment()
	defer conn.Close()

	conn.EnableStatementCache()
	_, err := conn.Open()
	assert.Nil(err)

	createTableStatement := `CREATE TABLE state_invalidation (id int not null, name varchar(64))`
	insertStatement := `INSERT INTO state_invalidation (id, name) VALUES ($1, $2)`
	alterTableStatement := `ALTER TABLE state_invalidation ALTER COLUMN id TYPE bigint;`
	dropTableStatement := `DROP TABLE state_invalidation`
	queryStatement := `SELECT * from state_invalidation`

	defer func() {
		err = conn.Exec(dropTableStatement)
		assert.Nil(err)
	}()

	err = conn.Exec(createTableStatement)
	assert.Nil(err)

	err = conn.Exec(insertStatement, 1, "Foo")
	assert.Nil(err)

	err = conn.Exec(insertStatement, 2, "Bar")
	assert.Nil(err)

	_, err = conn.Query(queryStatement).Any()
	assert.Nil(err)

	err = conn.Exec(alterTableStatement)
	assert.Nil(err)

	_, err = conn.Query(queryStatement).Any()
	assert.NotNil(err)

	_, err = conn.Query(queryStatement).Any()
	assert.Nil(err)
}
