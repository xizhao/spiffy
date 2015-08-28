package orm

import (
	"database/sql"
	"fmt"
	"gopkg.in/stretchr/testify.v1/assert"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	var db_server string = os.Getenv("BDS_DB_HOST")
	var db_schema string = os.Getenv("BDS_DB_SCHEMA")
	var db_user string = os.Getenv("BDS_DB_USER")
	var db_password string = os.Getenv("BDS_DB_PASSWORD")

	CreateDbAlias("main", NewDBConnection(db_server, db_schema, db_user, db_password))
	SetDefaultAlias("main")

	os.Exit(m.Run())
}

type BenchObj struct {
	Id        int       `db:"id,pk,serial"`
	Name      string    `db:"name"`
	Timestamp time.Time `db:"timestamp_utc"`
	Amount    float32   `db:"amount"`
	Pending   bool      `db:"pending"`
	Category  string    `db:"category"`
}

func (b *BenchObj) ManualPopulate(rows *sql.Rows) error {
	var id int
	var name string
	var ts time.Time
	var amount float32
	var pending bool
	var category string
	scan_err := rows.Scan(&id, &name, &ts, &amount, &pending, &category)

	if scan_err != nil {
		return scan_err
	}

	b.Id = id
	b.Name = name
	b.Timestamp = ts
	b.Amount = amount
	b.Pending = pending
	b.Category = category
	return nil
}

func (b BenchObj) TableName() string {
	return "bench_object"
}

func createTable(tx *sql.Tx) error {
	create_sql := `CREATE TABLE bench_object (id serial not null, name varchar(255), timestamp_utc timestamp, amount real, pending boolean, category varchar(255));`
	create_stmt, create_stmt_err := DefaultDb().Prepare(create_sql, tx)
	if create_stmt_err != nil {
		return create_stmt_err
	}
	_, exec_err := create_stmt.Exec()
	return exec_err
}

func createObject(index int, tx *sql.Tx) error {
	obj := BenchObj{}
	obj.Name = fmt.Sprintf("test_object_%d", index)
	obj.Timestamp = time.Now().UTC()
	obj.Amount = 1000.0 + (5.0 * float32(index))
	obj.Pending = index%2 == 0
	obj.Category = fmt.Sprintf("category_%d", index)
	return DefaultDb().CreateInTransaction(&obj, tx)
}

func seedObjects(count int, tx *sql.Tx) error {
	create_table_err := createTable(tx)

	if create_table_err != nil {
		return create_table_err
	}

	for i := 0; i < count; i++ {
		create_obj_err := createObject(i, tx)
		if create_obj_err != nil {
			return create_obj_err
		}
	}
	return nil
}

func readManual(tx *sql.Tx) ([]BenchObj, error) {
	objs := []BenchObj{}
	read_sql := `select id,name,timestamp_utc,amount,pending,category from bench_object`
	read_stmt, read_stmt_err := DefaultDb().Prepare(read_sql, tx)
	if read_stmt_err != nil {
		return nil, read_stmt_err
	}
	defer read_stmt.Close()

	rows, query_err := read_stmt.Query()
	defer rows.Close()
	if query_err != nil {
		return nil, query_err
	}

	for rows.Next() {
		obj := &BenchObj{}
		pop_err := obj.ManualPopulate(rows)
		if pop_err != nil {
			return nil, pop_err
		}
		objs = append(objs, *obj)
	}

	return objs, nil
}

func readOrm(tx *sql.Tx) ([]BenchObj, error) {
	objs := []BenchObj{}
	all_err := DefaultDb().GetAllInTransaction(&objs, tx)
	return objs, all_err
}

func TestBenchmarkComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	assert := assert.New(t)
	tx, tx_err := DefaultDb().Begin()
	assert.Nil(tx_err)
	defer tx.Rollback()

	seed_err := seedObjects(5000, tx)
	assert.Nil(seed_err)

	manual_before := time.Now()
	_, manual_err := readManual(tx)
	manual_after := time.Now()
	assert.Nil(manual_err)

	orm_before := time.Now()
	_, orm_err := readOrm(tx)
	orm_after := time.Now()
	assert.Nil(orm_err)

	t.Log(fmt.Sprintf("Orm Test Results: Manual: %v vs. Orm: %v\n", manual_after.Sub(manual_before), orm_after.Sub(orm_before)))
}

func TestAliases(t *testing.T) {
	assert := assert.New(t)

	var db_server string = os.Getenv("DB_HOST")
	var db_schema string = os.Getenv("DB_SCHEMA")
	var db_user string = os.Getenv("DB_USER")
	var db_password string = os.Getenv("DB_PASSWORD")

	conn := NewDBConnection(db_server, db_schema, db_user, db_password)
	CreateDbAlias("test", conn)

	got_conn := Alias("test")
	assert.Equal(conn.Username, got_conn.Username)

	SetDefaultAlias("test")
	default_conn := DefaultDb()
	assert.Equal(conn.Host, default_conn.Host)

	SetDefaultAlias("main")
}

func TestTransactionIsolation(t *testing.T) {
	assert := assert.New(t)

	tx, tx_err := DefaultDb().Begin()
	assert.Nil(tx_err)
	DefaultDb().IsolateToTransaction(tx)
	assert.True(DefaultDb().Tx != nil)
	assert.True(DefaultDb().IsIsolatedToTransaction())

	_, tx2_err := DefaultDb().Begin()
	assert.Nil(tx2_err)

	DefaultDb().ReleaseIsolation()
	assert.False(DefaultDb().IsIsolatedToTransaction())

	rollback_err := tx.Rollback()
	assert.Nil(rollback_err)
}

func TestPrepare(t *testing.T) {
	assert := assert.New(t)
	tx, tx_err := DefaultDb().Begin()
	assert.Nil(tx_err)
	//this command has a statment prep in it.
	create_table_err := createTable(tx)
	assert.Nil(create_table_err)
	tx.Rollback()
}

func TestQuery(t *testing.T) {
	assert := assert.New(t)
	tx, tx_err := DefaultDb().Begin()
	assert.Nil(tx_err)
	defer tx.Rollback()

	seed_err := seedObjects(100, tx)
	assert.Nil(seed_err)

	objs := []BenchObj{}
	query_err := DefaultDb().QueryInTransaction("select * from bench_object", tx).OutMany(&objs)

	assert.Nil(query_err)
	assert.NotEmpty(objs)

	all := []BenchObj{}
	all_err := DefaultDb().GetAllInTransaction(&all, tx)
	assert.Nil(all_err)
	assert.Equal(len(objs), len(all))

	obj := BenchObj{}
	single_query_err := DefaultDb().QueryInTransaction("select * from bench_object limit 1", tx).Out(&obj)
	assert.Nil(single_query_err)
	assert.True(obj.Id != 0)

	var id int
	scan_err := DefaultDb().QueryInTransaction("select id from bench_object limit 1", tx).Scan(&id)
	assert.Nil(scan_err)
	assert.True(id != 0)
}

func TestCrUDMethods(t *testing.T) {
	assert := assert.New(t)
	tx, tx_err := DefaultDb().Begin()
	assert.Nil(tx_err)
	defer tx.Rollback()

	seed_err := seedObjects(100, tx)
	assert.Nil(seed_err)

	objs := []BenchObj{}
	query_err := DefaultDb().QueryInTransaction("select * from bench_object", tx).OutMany(&objs)

	assert.Nil(query_err)
	assert.NotEmpty(objs)

	all := []BenchObj{}
	all_err := DefaultDb().GetAllInTransaction(&all, tx)
	assert.Nil(all_err)
	assert.Equal(len(objs), len(all))

	sample_obj := all[0]

	get_test := BenchObj{}
	get_test_err := DefaultDb().GetByIdInTransaction(&get_test, tx, sample_obj.Id)
	assert.Nil(get_test_err)
	assert.Equal(sample_obj.Id, get_test.Id)

	exists, exists_err := DefaultDb().ExistsInTransaction(&get_test, tx)
	assert.Nil(exists_err)
	assert.True(exists)

	get_test.Name = "not_a_test_object"

	update_err := DefaultDb().UpdateInTransaction(&get_test, tx)
	assert.Nil(update_err)

	verify := BenchObj{}
	verify_err := DefaultDb().GetByIdInTransaction(&verify, tx, get_test.Id)
	assert.Nil(verify_err)
	assert.Equal(get_test.Name, verify.Name)

	delete_err := DefaultDb().DeleteInTransaction(&verify, tx)
	assert.Nil(delete_err)

	del_verify := BenchObj{}
	del_verify_err := DefaultDb().GetByIdInTransaction(&del_verify, tx, get_test.Id)
	assert.Nil(del_verify_err)
}

type myStruct struct {
	PrimaryKeyCol     int    `json:"pk" db:"primary_key_column,pk,serial"`
	InferredName      string `json:"normal"`
	Excluded          string `json:"-" db:"-"`
	NullableCol       string `json:"not_nullable" db:"nullable,nullable"`
	InferredWithFlags string `db:",readonly"`
}

func (m myStruct) TableName() string {
	return "my_struct"
}

func TestGetColumns(t *testing.T) {
	assert := assert.New(t)

	obj := myStruct{}

	meta := GetColumns(obj)

	assert.NotNil(meta.Columns)
	assert.NotEmpty(meta.Columns)

	assert.Equal(4, len(meta.Columns))

	first_col := meta.Columns[0]
	assert.Equal("my_struct", first_col.TableName)
	assert.Equal("PrimaryKeyCol", first_col.FieldName)
	assert.Equal("primary_key_column", first_col.ColumnName)
	assert.True(first_col.IsPrimaryKey)
	assert.True(first_col.IsSerial)
	assert.False(first_col.IsNullable)
	assert.False(first_col.IsReadOnly)

	second_col := meta.Columns[1]
	assert.Equal("inferredname", second_col.ColumnName)
	assert.False(second_col.IsPrimaryKey)
	assert.False(second_col.IsSerial)
	assert.False(second_col.IsNullable)
	assert.False(second_col.IsReadOnly)

	third_col := meta.Columns[2]
	assert.Equal("nullable", third_col.ColumnName)
	assert.False(third_col.IsPrimaryKey)
	assert.False(third_col.IsSerial)
	assert.True(third_col.IsNullable)
	assert.False(third_col.IsReadOnly)

	fourth_col := meta.Columns[3]
	assert.Equal("inferredwithflags", fourth_col.ColumnName)
	assert.False(fourth_col.IsPrimaryKey)
	assert.False(fourth_col.IsSerial)
	assert.False(fourth_col.IsNullable)
	assert.True(fourth_col.IsReadOnly)
}

func TestSetValue(t *testing.T) {
	assert := assert.New(t)
	obj := myStruct{InferredName: "Hello."}
	var value interface{}
	value = 10
	meta := GetColumns(obj)
	pk := meta.Columns[0]
	pk.SetValue(&obj, value)
	assert.Equal(10, obj.PrimaryKeyCol)
}

func TestMakeCsvTokens(t *testing.T) {
	assert := assert.New(t)

	one := makeCsvTokens(1)
	two := makeCsvTokens(2)
	three := makeCsvTokens(3)

	assert.Equal("$1", one)
	assert.Equal("$1,$2", two)
	assert.Equal("$1,$2,$3", three)
}
