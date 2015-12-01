// Package spiffy providers a basic abstraction layer above normal database/sql that makes it easier to
// interact with the database and organize database related code. It is not intended to replace actual sql
// (you write queries yourself).
package spiffy

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	_ "github.com/lib/pq"
)

var metaCacheLock sync.Mutex = sync.Mutex{}
var metaCache map[reflect.Type]columnCollection

var defaultAlias string
var defaultAliasLock sync.Mutex = sync.Mutex{}
var dbAliasesLock sync.Mutex = sync.Mutex{}
var dbAliases map[string]*DbConnection = make(map[string]*DbConnection)

// DatabaseMapped is the interface that any objects passed into database mapped methods like Create, Update, Delete, Get, GetAll etc.
// The only required method is TableName() string that returns the name of the table in the database this type is mapped to.
//
//	type MyDatabaseMappedObject {
//		Mycolumn `db:"my_column"`
//	}
//	func (_ MyDatabaseMappedObject) TableName() string {
//		return "my_database_mapped_object"
//	}
// If you require different table names based on alias, create another type.
type DatabaseMapped interface {
	TableName() string
}

// CreateDbAlias allows you to set up a connection for later use via an alias.
//
//	spiffy.CreateDbAlias("main", spiffy.NewDbConnection("localhost", "test_db", "", ""))
//
// You can later set an alias as 'default' and refer to it using `spiffy.DefaultDb()`.
func CreateDbAlias(alias string, prototype *DbConnection) {
	dbAliasesLock.Lock()
	dbAliases[alias] = prototype
	dbAliasesLock.Unlock()
}

// Fetch a connection by its alias.
//
//	spiffy.Alias("logging").Create(&object)
//
// Alternately, if you've set the alias as 'default' you can just refer to it via. `DefaultDb()`
func Alias(alias string) *DbConnection {
	return dbAliases[alias]
}

// Sets an alias created with `CreateDbAlias` as default. This lets you refer to it later via. `DefaultDb()`
//
//	spiffy.CreateDbAlias("main", spiffy.NewDbConnection("localhost", "test_db", "", ""))
//	spiffy.SetDefaultAlias("main")
//	execErr := spiffy.DefaultDb().Execute("select 'ok!')
//
// This will then let you refer to the alias via. `DefaultDb()`
func SetDefaultAlias(alias string) {
	defaultAliasLock.Lock()
	defaultAlias = alias
	defaultAliasLock.Unlock()
}

// Returns a reference to the DbConnection set as default.
//
//	spiffy.DefaultDb().Exec("select 'ok!")
//
// Note: you must set up the default with `SetDefaultAlias()` before using DefaultDb.
func DefaultDb() *DbConnection {
	if len(defaultAlias) != 0 {
		return dbAliases[defaultAlias]
	} else {
		return nil
	}
}

// Creates a new DbConnection without Username or Password or SSLMode
func NewUnauthenticatedDbConnection(host, schema string) *DbConnection {
	conn := &DbConnection{}
	conn.Host = host
	conn.Schema = schema
	conn.Username = ""
	conn.Password = ""
	conn.SSLMode = "disable"
	return conn
}

// Creates a new connection with SSLMode set to "disable"
func NewDbConnection(host, schema, username, password string) *DbConnection {
	conn := &DbConnection{}
	conn.Host = host
	conn.Schema = schema
	conn.Username = username
	conn.Password = password
	conn.SSLMode = "disable"
	return conn
}

// Creates a new connection with all available options (including SSLMode)
func NewSSLDbConnection(host, schema, username, password, sslMode string) *DbConnection {
	conn := &DbConnection{}
	conn.Host = host
	conn.Schema = schema
	conn.Username = username
	conn.Password = password
	conn.SSLMode = sslMode
	return conn
}

// DbConnection is the basic wrapper for connection parameters and saves a reference to the created sql.Connection.
type DbConnection struct {
	Host       string
	Schema     string
	Username   string
	Password   string
	SSLMode    string
	Connection *sql.DB
	Tx         *sql.Tx
}

// --------------------------------------------------------------------------------
// Column
// --------------------------------------------------------------------------------

type column struct {
	TableName    string
	FieldName    string
	FieldType    reflect.Type
	ColumnName   string
	Index        int
	IsPrimaryKey bool
	IsSerial     bool
	IsNullable   bool
	IsReadOnly   bool
	IsJson       bool
}

func (c column) SetValue(object DatabaseMapped, value interface{}) error {
	objValue := reflectValue(object)
	field := objValue.FieldByName(c.FieldName)
	fieldType := field.Type()
	if field.CanSet() {
		valueReflected := reflectValue(value)
		if valueReflected.IsValid() {
			if c.IsJson {
				valueAsString, ok := valueReflected.Interface().(string)
				if ok && len(valueAsString) != 0 {
					fieldAddr := field.Addr().Interface()
					jsonErr := json.Unmarshal([]byte(valueAsString), fieldAddr)
					if jsonErr != nil {
						return jsonErr
					}
					field.Set(reflect.ValueOf(fieldAddr).Elem())
				}
			} else {
				if valueReflected.Type().AssignableTo(fieldType) {
					if field.Kind() == reflect.Ptr && valueReflected.CanAddr() {
						field.Set(valueReflected.Addr())
					} else {
						field.Set(valueReflected)
					}
				} else {
					if field.Kind() == reflect.Ptr {
						if valueReflected.CanAddr() {
							if fieldType.Elem() == valueReflected.Type() {
								field.Set(valueReflected.Addr())
							} else {
								convertedValue := valueReflected.Convert(fieldType.Elem())
								if convertedValue.CanAddr() {
									field.Set(convertedValue.Addr())
								}
							}
						}
					} else {
						convertedValue := valueReflected.Convert(fieldType)
						field.Set(convertedValue)
					}
				}
			}
		}
	} else {
		return errors.New("hit a field we can't set: '" + c.FieldName + "', did you forget to pass the object as a reference?")
	}
	return nil
}

func (c column) GetValue(object DatabaseMapped) interface{} {
	value := reflectValue(object)
	valueField := value.Field(c.Index)
	return valueField.Interface()
}

// --------------------------------------------------------------------------------
// Column Collection
// --------------------------------------------------------------------------------

type columnCollection struct {
	Columns []column
	Lookup  map[string]*column
}

func newcolumnCollection(columns []column) columnCollection {
	cc := columnCollection{Columns: columns}
	lookup := make(map[string]*column)
	for i := 0; i < len(columns); i++ {
		col := &columns[i]
		lookup[col.ColumnName] = col
	}
	cc.Lookup = lookup
	return cc
}

//things we use as where predicates and can't update
func (cc columnCollection) PrimaryKeys() columnCollection {
	var cols []column
	for _, c := range cc.Columns {
		if c.IsPrimaryKey {
			cols = append(cols, c)
		}
	}
	return newcolumnCollection(cols)
}

//things we can update
func (cc columnCollection) NotPrimaryKeys() columnCollection {
	var cols []column
	for _, c := range cc.Columns {
		if !c.IsPrimaryKey {
			cols = append(cols, c)
		}
	}
	return newcolumnCollection(cols)
}

//things we have to return the id of ...
func (cc columnCollection) Serials() columnCollection {
	var cols []column
	for _, c := range cc.Columns {
		if c.IsSerial {
			cols = append(cols, c)
		}
	}
	return newcolumnCollection(cols)
}

//things we don't have to return the id of ...
func (cc columnCollection) NotSerials() columnCollection {
	var cols []column
	for _, c := range cc.Columns {
		if !c.IsSerial {
			cols = append(cols, c)
		}
	}
	return newcolumnCollection(cols)
}

//a.k.a. not things we insert
func (cc columnCollection) ReadOnly() columnCollection {
	var cols []column
	for _, c := range cc.Columns {
		if c.IsReadOnly {
			cols = append(cols, c)
		}
	}
	return newcolumnCollection(cols)
}

func (cc columnCollection) NotReadonly() columnCollection {
	var cols []column
	for _, c := range cc.Columns {
		if !c.IsReadOnly {
			cols = append(cols, c)
		}
	}
	return newcolumnCollection(cols)
}

func (cc columnCollection) ColumnNames() []string {
	var names []string
	for _, c := range cc.Columns {
		names = append(names, c.ColumnName)
	}
	return names
}

func (cc columnCollection) ColumnValues(instance interface{}) []interface{} {
	value := reflectValue(instance)

	var values []interface{}
	for _, c := range cc.Columns {
		valueField := value.FieldByName(c.FieldName)
		if c.IsJson {
			toSerialize := valueField.Interface()
			jsonBytes, _ := json.Marshal(toSerialize)
			values = append(values, string(jsonBytes))
		} else {
			values = append(values, valueField.Interface())
		}

	}
	return values
}

func (cc columnCollection) FirstOrDefault() *column {
	if len(cc.Columns) > 0 {
		col := cc.Columns[0]
		return &col
	}
	return nil
}

func (cc columnCollection) ConcatWith(other columnCollection) columnCollection {
	var total []column
	total = append(total, cc.Columns...)
	total = append(total, other.Columns...)
	return newcolumnCollection(total)
}

// --------------------------------------------------------------------------------
// Query Result
// --------------------------------------------------------------------------------

type queryResult struct {
	Rows  *sql.Rows
	Stmt  *sql.Stmt
	Error error
}

func (q *queryResult) Scan(args ...interface{}) error {
	if q.Error != nil {
		if q.Rows != nil {
			q.Rows.Close()
		}
		if q.Stmt != nil {
			q.Stmt.Close()
		}

		return q.Error
	}

	if q.Rows.Next() {
		err := q.Rows.Scan(args...)
		if err != nil {
			return err
		}
	}

	if q.Rows != nil {
		q.Rows.Close()
	}
	if q.Stmt != nil {
		q.Stmt.Close()
	}

	return nil
}

func (q *queryResult) Out(object DatabaseMapped) error {

	if q.Error != nil {
		if q.Rows != nil {
			q.Rows.Close()
		}
		if q.Stmt != nil {
			q.Stmt.Close()
		}

		return q.Error
	}

	meta := getColumns(object)

	if q.Rows.Next() {
		pop_err := populateByName(object, q.Rows, meta)
		if pop_err != nil {
			if q.Rows != nil {
				q.Rows.Close()
			}
			if q.Stmt != nil {
				q.Stmt.Close()
			}

			return pop_err
		}
	}

	if q.Rows != nil {
		q.Rows.Close()
	}
	if q.Stmt != nil {
		q.Stmt.Close()
	}

	return nil
}

func (q *queryResult) OutMany(collection interface{}) error {
	if q.Error != nil {
		if q.Rows != nil {
			q.Rows.Close()
		}
		if q.Stmt != nil {
			q.Stmt.Close()
		}

		return q.Error
	}

	collection_value := reflectValue(collection)
	t := reflectSliceType(collection)
	slice_t := reflectType(collection)
	meta := getColumnsByType(t)

	did_set_rows := false
	for q.Rows.Next() {
		new_obj := makeNew(t)
		pop_err := populateByName(new_obj, q.Rows, meta)
		if pop_err != nil {
			if q.Rows != nil {
				q.Rows.Close()
			}
			if q.Stmt != nil {
				q.Stmt.Close()
			}

			return pop_err
		}
		new_obj_value := reflectValue(new_obj)
		collection_value.Set(reflect.Append(collection_value, new_obj_value))
		did_set_rows = true
	}

	if !did_set_rows {
		collection_value.Set(reflect.MakeSlice(slice_t, 0, 0))
	}

	if q.Rows != nil {
		q.Rows.Close()
	}
	if q.Stmt != nil {
		q.Stmt.Close()
	}

	return nil
}

// --------------------------------------------------------------------------------
// DbConnection
// --------------------------------------------------------------------------------

// Returns a sql connection string from a given set of DbConnection parameters
func (dbAlias *DbConnection) CreatePostgresConnectionString() string {
	sslMode := "?sslmode=disable"
	if dbAlias.SSLMode != "" {
		sslMode = fmt.Sprintf("?sslmode=%s", dbAlias.SSLMode)
	}

	if dbAlias.Username != "" {
		if dbAlias.Password != "" {
			return fmt.Sprintf("postgres://%s:%s@%s/%s%s", dbAlias.Username, dbAlias.Password, dbAlias.Host, dbAlias.Schema, sslMode)
		} else {
			return fmt.Sprintf("postgres://%s@%s/%s%s", dbAlias.Username, dbAlias.Host, dbAlias.Schema, sslMode)
		}
	} else {
		return fmt.Sprintf("postgres://%s/%s%s", dbAlias.Host, dbAlias.Schema, sslMode)
	}
}

// Isolates a DbConnection, globally, to a transaction. This means that any operations called after this method will use the same transaction.
func (dbAlias *DbConnection) IsolateToTransaction(tx *sql.Tx) {
	dbAlias.Tx = tx
}

// Releases an isolation, does not commit or rollback.
func (dbAlias *DbConnection) ReleaseIsolation() {
	dbAlias.Tx = nil
}

// Indicates if a connection is isolated to a transaction.
func (dbAlias *DbConnection) IsIsolatedToTransaction() bool {
	return dbAlias.Tx != nil
}

// Starts a new transaction.
func (dbAlias *DbConnection) Begin() (*sql.Tx, error) {
	if dbAlias == nil {
		return nil, errors.New("`dbAlias` is uninitialized, cannot continue.")
	}

	if dbAlias.Tx != nil {
		return dbAlias.Tx, nil
	} else if dbAlias.Connection != nil {
		return dbAlias.Connection.Begin()
	} else {
		db_conn, open_err := dbAlias.OpenNew()
		if open_err != nil {
			return nil, open_err
		}
		dbAlias.Connection = db_conn
		return dbAlias.Connection.Begin()
	}
}

// Performs the given action wrapped in a transaction. Will Commit() on success and Rollback() on a non-nil error returned.
func (dbAlias *DbConnection) WrapInTransaction(action func(*sql.Tx) error) error {
	tx, err := dbAlias.Begin()
	if err != nil {
		return err
	}
	err = action(tx)
	if dbAlias.IsIsolatedToTransaction() {
		return err
	}
	if err != nil {
		tx.Rollback()
	} else {
		tx.Commit()
	}
	return err
}

// Prepares a new statement for the connection.
func (dbAlias *DbConnection) Prepare(statement string, tx *sql.Tx) (*sql.Stmt, error) {
	if tx == nil {
		if dbAlias == nil {
			return nil, errors.New("dbAlias is nil")
		}

		if dbAlias.Tx != nil {
			return dbAlias.Tx.Prepare(statement)
		} else {
			db_conn, db_err := dbAlias.Open()
			if db_err != nil {
				return nil, db_err
			} else {
				return db_conn.Prepare(statement)
			}
		}
	} else {
		return tx.Prepare(statement)
	}
}

func (dbAlias *DbConnection) OpenNew() (*sql.DB, error) {
	if dbConn, err := sql.Open("postgres", dbAlias.CreatePostgresConnectionString()); err != nil {
		return nil, err
	} else {
		return dbConn, nil
	}
}

func (dbAlias *DbConnection) Open() (*sql.DB, error) {
	if dbAlias.Connection == nil {
		if new_conn, err := dbAlias.OpenNew(); err != nil {
			return nil, err
		} else {
			dbAlias.Connection = new_conn
		}
	}
	return dbAlias.Connection, nil
}

func (dbAlias *DbConnection) Exec(statement string, args ...interface{}) error {
	return dbAlias.ExecInTransaction(statement, nil, args...)
}

func (dbAlias *DbConnection) ExecInTransaction(statement string, tx *sql.Tx, args ...interface{}) error {
	stmt, stmt_err := dbAlias.Prepare(statement, tx)

	if stmt_err != nil {
		return stmt_err
	}
	defer stmt.Close()

	_, exec_err := stmt.Exec(args...)
	if exec_err != nil {
		return exec_err
	}

	return nil
}

func (dbAlias *DbConnection) Query(statement string, args ...interface{}) *queryResult {
	return dbAlias.QueryInTransaction(statement, nil, args...)
}

func (dbAlias *DbConnection) QueryInTransaction(statement string, tx *sql.Tx, args ...interface{}) *queryResult {
	result := queryResult{}

	stmt, stmt_err := dbAlias.Prepare(statement, tx)
	if stmt_err != nil {
		result.Error = stmt_err
		return &result
	}

	rows, query_err := stmt.Query(args...)
	if query_err != nil {
		stmt.Close()
		result.Error = query_err
		return &result
	}
	result.Stmt = stmt
	result.Rows = rows
	return &result
}

func (dbAlias *DbConnection) GetById(object DatabaseMapped, ids ...interface{}) error {
	return dbAlias.GetByIdInTransaction(object, nil, ids...)
}

func (dbAlias *DbConnection) GetByIdInTransaction(object DatabaseMapped, tx *sql.Tx, ids ...interface{}) error {
	if ids == nil {
		return errors.New("invalid `ids` parameter.")
	}

	meta := getColumns(object)
	standard_cols := meta.NotReadonly()
	column_names := standard_cols.ColumnNames()
	table_name := object.TableName()
	pks := standard_cols.PrimaryKeys()

	if len(pks.Columns) == 0 {
		return errors.New("no primary key on object to get by.")
	}

	where_clause := makeWhereClause(pks, 1)
	query_body := fmt.Sprintf("SELECT %s FROM %s %s", strings.Join(column_names, ","), table_name, where_clause)

	stmt, stmt_err := dbAlias.Prepare(query_body, tx)
	if stmt_err != nil {
		return stmt_err
	}
	defer stmt.Close()

	rows, query_err := stmt.Query(ids...)
	if query_err != nil {
		return query_err
	}

	defer rows.Close()

	for rows.Next() {
		pop_err := populateInOrder(object, rows, standard_cols)
		if pop_err != nil {
			return pop_err
		}
	}

	return nil
}

func (dbAlias *DbConnection) GetAll(collection interface{}) error {
	return dbAlias.GetAllInTransaction(collection, nil)
}

func (dbAlias *DbConnection) GetAllInTransaction(collection interface{}, tx *sql.Tx) error {
	collection_value := reflectValue(collection)
	t := reflectSliceType(collection)
	table_name := tableName(t)
	meta := getColumnsByType(t).NotReadonly()

	column_names := meta.ColumnNames()

	sql_stmt := fmt.Sprintf("SELECT %s FROM %s", strings.Join(column_names, ","), table_name)

	stmt, statment_err := dbAlias.Prepare(sql_stmt, tx)
	if statment_err != nil {
		return statment_err
	}
	defer stmt.Close()

	rows, query_err := stmt.Query()
	if query_err != nil {
		return query_err
	}

	defer rows.Close()

	for rows.Next() {
		new_obj := makeNew(t)
		pop_err := populateInOrder(new_obj, rows, meta)
		if pop_err != nil {
			return pop_err
		}
		new_obj_value := reflectValue(new_obj)
		collection_value.Set(reflect.Append(collection_value, new_obj_value))
	}

	return nil
}

func (dbAlias *DbConnection) Create(object DatabaseMapped) error {
	return dbAlias.CreateInTransaction(object, nil)
}

func (dbAlias *DbConnection) CreateInTransaction(object DatabaseMapped, tx *sql.Tx) error {
	cols := getColumns(object)
	write_cols := cols.NotReadonly().NotSerials()
	//NOTE: we're only using one.
	serials := cols.Serials()
	table_name := object.TableName()
	col_names := write_cols.ColumnNames()
	col_values := write_cols.ColumnValues(object)
	tokens := makeCsvTokens(len(write_cols.Columns))

	var sql_stmt string
	if len(serials.Columns) == 0 {
		sql_stmt = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			table_name,
			strings.Join(col_names, ","),
			tokens,
		)
	} else {
		serial := serials.Columns[0]
		sql_stmt = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
			table_name,
			strings.Join(col_names, ","),
			tokens,
			serial.ColumnName,
		)
	}

	stmt, stmt_err := dbAlias.Prepare(sql_stmt, tx)
	if stmt_err != nil {
		return stmt_err
	}
	defer stmt.Close()

	if len(serials.Columns) == 0 {
		_, exec_err := stmt.Exec(col_values...)
		if exec_err != nil {
			return exec_err
		}
	} else {
		serial := serials.Columns[0]

		var id interface{}
		exec_err := stmt.QueryRow(col_values...).Scan(&id)
		if exec_err != nil {
			return exec_err
		}
		set_err := serial.SetValue(object, id)
		if set_err != nil {
			return set_err
		}
	}

	return nil
}

func (dbAlias *DbConnection) Update(object DatabaseMapped) error {
	return dbAlias.UpdateInTransaction(object, nil)
}

func (dbAlias *DbConnection) UpdateInTransaction(object DatabaseMapped, tx *sql.Tx) error {
	table_name := object.TableName()

	cols := getColumns(object)
	write_cols := cols.NotReadonly().NotSerials().NotPrimaryKeys()
	pks := cols.PrimaryKeys()
	all_cols := write_cols.ConcatWith(pks)

	total_values := all_cols.ColumnValues(object)

	number_of_columns := len(write_cols.Columns)

	sql_stmt := "UPDATE " + table_name + " SET "
	for i, col := range write_cols.Columns {
		sql_stmt = sql_stmt + col.ColumnName + " = $" + strconv.Itoa(i+1)
		if i != number_of_columns-1 {
			sql_stmt = sql_stmt + ","
		}
	}

	where_clause := makeWhereClause(pks, number_of_columns+1)

	sql_stmt = sql_stmt + where_clause

	stmt, stmt_err := dbAlias.Prepare(sql_stmt, tx)
	if stmt_err != nil {
		return stmt_err
	}
	defer stmt.Close()
	_, err := stmt.Exec(total_values...)

	if err != nil {
		return err
	}

	return nil
}

func (dbAlias *DbConnection) Exists(object DatabaseMapped) (bool, error) {
	return dbAlias.ExistsInTransaction(object, nil)
}

func (dbAlias *DbConnection) ExistsInTransaction(object DatabaseMapped, tx *sql.Tx) (bool, error) {
	table_name := object.TableName()
	cols := getColumns(object)
	pks := cols.PrimaryKeys()

	if len(pks.Columns) == 0 {
		return false, errors.New("No primary key on object.")
	}
	where_clause := makeWhereClause(pks, 1)
	sql_stmt := fmt.Sprintf("SELECT 1 FROM %s %s", table_name, where_clause)
	stmt, stmt_err := dbAlias.Prepare(sql_stmt, tx)
	if stmt_err != nil {
		return false, stmt_err
	}
	defer stmt.Close()
	pk_values := pks.ColumnValues(object)
	rows, query_err := stmt.Query(pk_values...)
	defer rows.Close()
	if query_err != nil {
		return false, query_err
	}

	exists := rows.Next()
	return exists, nil
}

func (dbAlias *DbConnection) Delete(object DatabaseMapped) error {
	return dbAlias.DeleteInTransaction(object, nil)
}

func (dbAlias *DbConnection) DeleteInTransaction(object DatabaseMapped, tx *sql.Tx) error {
	table_name := object.TableName()
	cols := getColumns(object)
	pks := cols.PrimaryKeys()

	if len(pks.Columns) == 0 {
		return errors.New("No primary key on object.")
	}

	where_clause := makeWhereClause(pks, 1)
	sql_stmt := fmt.Sprintf("DELETE FROM %s %s", table_name, where_clause)

	stmt, stmt_err := dbAlias.Prepare(sql_stmt, tx)
	if stmt_err != nil {
		return stmt_err
	}
	defer stmt.Close()

	pk_values := pks.ColumnValues(object)

	_, err := stmt.Exec(pk_values...)

	if err != nil {
		return err
	}
	return nil
}

// --------------------------------------------------------------------------------
// Utility Methods
// --------------------------------------------------------------------------------

func reflectValue(obj interface{}) reflect.Value {
	v := reflect.ValueOf(obj)
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return v
}

func reflectType(obj interface{}) reflect.Type {
	t := reflect.TypeOf(obj)
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Interface {
		t = t.Elem()
	}

	return t
}

func reflectSliceType(collection interface{}) reflect.Type {
	t := reflect.TypeOf(collection)
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Interface || t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	return t
}

func makeWhereClause(pks columnCollection, start_at int) string {
	where_clause := " WHERE "
	for i, pk := range pks.Columns {
		where_clause = where_clause + fmt.Sprintf("%s = %s", pk.ColumnName, "$"+strconv.Itoa(i+start_at))
		if i < (len(pks.Columns) - 1) {
			where_clause = where_clause + " AND "
		}
	}

	return where_clause
}

func makeCsvTokens(num int) string {
	str := ""
	for i := 1; i <= num; i++ {
		str = str + fmt.Sprintf("$%d", i)
		if i != num {
			str = str + ","
		}
	}
	return str
}

func tableName(t reflect.Type) string {
	return makeNew(t).TableName()
}

func makeNew(t reflect.Type) DatabaseMapped {
	new_interface := reflect.New(t).Interface()
	return new_interface.(DatabaseMapped)
}

func makeSliceOfType(t reflect.Type) interface{} {
	return reflect.New(reflect.SliceOf(t)).Interface()
}

func populate(object DatabaseMapped, row *sql.Rows) error {
	return populateByName(object, row, getColumns(object))
}

func populateByName(object DatabaseMapped, row *sql.Rows, cols columnCollection) error {
	row_columns, row_columns_err := row.Columns()

	if row_columns_err != nil {
		return row_columns_err
	}

	var values = make([]interface{}, len(row_columns))

	for i, name := range row_columns {
		if col, ok := cols.Lookup[name]; ok {
			if col.IsJson {
				str := ""
				values[i] = &str
			} else {
				values[i] = reflect.New(reflect.PtrTo(col.FieldType)).Interface()
			}
		} else {
			var value interface{}
			values[i] = &value
		}
	}

	scan_err := row.Scan(values...)

	if scan_err != nil {
		return scan_err
	}

	for i, v := range values {
		col_name := row_columns[i]

		if field, ok := cols.Lookup[col_name]; ok {
			err := field.SetValue(object, v)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func populateInOrder(object DatabaseMapped, row *sql.Rows, cols columnCollection) error {
	var values = make([]interface{}, len(cols.Columns))

	for i, col := range cols.Columns {
		if col.FieldType.Kind() == reflect.Ptr {
			if col.IsJson {
				str := ""
				values[i] = &str
			} else {
				blank_ptr := reflect.New(reflect.PtrTo(col.FieldType))
				if blank_ptr.CanAddr() {
					values[i] = blank_ptr.Addr()
				} else {
					values[i] = blank_ptr.Interface()
				}
			}
		} else {
			if col.IsJson {
				str := ""
				values[i] = &str
			} else {
				values[i] = reflect.New(reflect.PtrTo(col.FieldType)).Interface()
			}
		}
	}

	scan_err := row.Scan(values...)

	if scan_err != nil {
		return scan_err
	}

	for i, v := range values {
		field := cols.Columns[i]
		err := field.SetValue(object, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func getColumns(object DatabaseMapped) columnCollection {
	return getColumnsByType(reflect.TypeOf(object))
}

func getColumnsByType(t reflect.Type) columnCollection {
	metaCacheLock.Lock()
	defer metaCacheLock.Unlock()
	if metaCache == nil {
		metaCache = map[reflect.Type]columnCollection{}
	}

	if _, ok := metaCache[t]; !ok {
		metaCache[t] = createColumnsByType(t)
	}
	return metaCache[t]
}

func createColumnsByType(t reflect.Type) columnCollection {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	table_name := tableName(t)

	number_of_fields := t.NumField()

	var cols []column
	for index := 0; index < number_of_fields; index++ {
		field := t.Field(index)
		if !field.Anonymous {
			col := readFieldTag(field)
			if col != nil {
				col.Index = index
				col.TableName = table_name
				cols = append(cols, *col)
			}
		}
	}

	return newcolumnCollection(cols)
}

// reads the contents of a field tag, ex: `json:"foo" db:"bar,isprimarykey,isserial"
func readFieldTag(field reflect.StructField) *column {
	db := field.Tag.Get("db")
	if db != "-" {
		col := column{}
		col.FieldName = field.Name
		col.ColumnName = strings.ToLower(field.Name)
		col.FieldType = field.Type
		if db != "" {
			pieces := strings.Split(db, ",")

			if !strings.HasPrefix(db, ",") {
				col.ColumnName = pieces[0]
			}

			if len(pieces) >= 1 {
				args := strings.Join(pieces[1:], ",")
				col.IsPrimaryKey = strings.Contains(strings.ToLower(args), "pk")
				col.IsSerial = strings.Contains(strings.ToLower(args), "serial")
				col.IsNullable = strings.Contains(strings.ToLower(args), "nullable")
				col.IsReadOnly = strings.Contains(strings.ToLower(args), "readonly")
				col.IsJson = strings.Contains(strings.ToLower(args), "json")
			}
		}
		return &col
	}

	return nil
}
