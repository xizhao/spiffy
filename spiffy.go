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
var metaCache map[reflect.Type]ColumnCollection

var defaultAlias string
var defaultAliasLock sync.Mutex = sync.Mutex{}
var dbAliasesLock sync.Mutex = sync.Mutex{}
var dbAliases map[string]*DBConnection = make(map[string]*DBConnection)

func CreateDbAlias(alias string, prototype *DBConnection) {
	dbAliasesLock.Lock()
	dbAliases[alias] = prototype
	dbAliasesLock.Unlock()
}

func Alias(alias string) *DBConnection {
	return dbAliases[alias]
}

func SetDefaultAlias(alias string) {
	defaultAliasLock.Lock()
	defaultAlias = alias
	defaultAliasLock.Unlock()
}

func DefaultDb() *DBConnection {
	if len(defaultAlias) != 0 {
		return dbAliases[defaultAlias]
	} else {
		return nil
	}
}

func NewDBConnection(host string, schema string, username string, password string) *DBConnection {
	conn := &DBConnection{}
	conn.Host = host
	conn.Schema = schema
	conn.Username = username
	conn.Password = password
	conn.SSLMode = "disable"
	return conn
}

type DBConnection struct {
	Host       string
	Schema     string
	Username   string
	Password   string
	SSLMode    string
	Connection *sql.DB
	Tx         *sql.Tx
}

type QueryResult struct {
	Rows  *sql.Rows
	Stmt  *sql.Stmt
	Error error
}

type DatabaseMapped interface {
	TableName() string
}

type Column struct {
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

type ColumnCollection struct {
	Columns []Column
	Lookup  map[string]*Column
}

func (db_alias *DBConnection) IsolateToTransaction(tx *sql.Tx) {
	db_alias.Tx = tx
}

func (db_alias *DBConnection) ReleaseIsolation() {
	db_alias.Tx = nil
}

func (db_alias *DBConnection) IsIsolatedToTransaction() bool {
	return db_alias.Tx != nil
}

func (db_alias *DBConnection) Begin() (*sql.Tx, error) {
	if db_alias == nil {
		return nil, errors.New("`db_alias` is uninitialized, cannot continue.")
	}

	if db_alias.Tx != nil {
		return db_alias.Tx, nil
	} else if db_alias.Connection != nil {
		return db_alias.Connection.Begin()
	} else {
		db_conn, open_err := db_alias.OpenNew()
		if open_err != nil {
			return nil, open_err
		}
		db_alias.Connection = db_conn
		return db_alias.Connection.Begin()
	}
}

func (db_alias *DBConnection) WrapInTransaction(action func(*sql.Tx) error) error {
	tx, err := db_alias.Begin()
	if err != nil {
		return err
	}
	err = action(tx)
	if db_alias.IsIsolatedToTransaction() {
		return err
	}
	if err != nil {
		tx.Rollback()
	} else {
		tx.Commit()
	}
	return err
}

func (db_alias *DBConnection) Prepare(statement string, tx *sql.Tx) (*sql.Stmt, error) {
	if tx == nil {
		if db_alias == nil {
			return nil, errors.New("db_alias is nil")
		}

		if db_alias.Tx != nil {
			return db_alias.Tx.Prepare(statement)
		} else {
			db_conn, db_err := db_alias.Open()
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

func (db_alias *DBConnection) CreatePostgresConnectionString() string {
	if db_alias.Username != "" {
		if db_alias.Password != "" {
			return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s", db_alias.Username, db_alias.Password, db_alias.Host, db_alias.Schema, db_alias.SSLMode)
		} else {
			return fmt.Sprintf("postgres://%s@%s/%s?sslmode=%s", db_alias.Username, db_alias.Host, db_alias.Schema, db_alias.SSLMode)
		}
	} else {
		return fmt.Sprintf("postgres://%s/%s?sslmode=%s", db_alias.Host, db_alias.Schema, db_alias.SSLMode)
	}
}

func (db_alias *DBConnection) OpenNew() (*sql.DB, error) {
	db_conn, err := sql.Open("postgres", db_alias.CreatePostgresConnectionString())

	if err != nil {
		return nil, err
	}

	return db_conn, nil
}

func (db_alias *DBConnection) Open() (*sql.DB, error) {
	if db_alias.Connection == nil {
		new_conn, err := db_alias.OpenNew()
		if err != nil {
			return nil, err
		} else {
			db_alias.Connection = new_conn
		}
	}
	return db_alias.Connection, nil
}

func (db_alias *DBConnection) Exec(statement string, args ...interface{}) error {
	return db_alias.ExecInTransaction(statement, nil, args...)
}

func (db_alias *DBConnection) ExecInTransaction(statement string, tx *sql.Tx, args ...interface{}) error {
	stmt, stmt_err := db_alias.Prepare(statement, tx)

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

func (db_alias *DBConnection) Query(statement string, args ...interface{}) *QueryResult {
	return db_alias.QueryInTransaction(statement, nil, args...)
}

func (db_alias *DBConnection) QueryInTransaction(statement string, tx *sql.Tx, args ...interface{}) *QueryResult {
	result := QueryResult{}

	stmt, stmt_err := db_alias.Prepare(statement, tx)
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

//STANDARD FUNCTIONS ==> GetById, GetAll, Create, Update, Delete
func (db_alias DBConnection) GetById(object DatabaseMapped, ids ...interface{}) error {
	return db_alias.GetByIdInTransaction(object, nil, ids...)
}

func (db_alias *DBConnection) GetByIdInTransaction(object DatabaseMapped, tx *sql.Tx, ids ...interface{}) error {
	if ids == nil {
		errors.New("invalid `ids` parameter.")
	}

	meta := GetColumns(object)
	standard_cols := meta.NotReadonly()
	column_names := standard_cols.ColumnNames()
	table_name := object.TableName()
	pks := standard_cols.PrimaryKeys()

	if len(pks.Columns) == 0 {
		return errors.New("no primary key on object to get by.")
	}

	where_clause := makeWhereClause(pks, 1)
	query_body := fmt.Sprintf("SELECT %s FROM %s %s", strings.Join(column_names, ","), table_name, where_clause)

	stmt, stmt_err := db_alias.Prepare(query_body, tx)
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
		pop_err := PopulateInOrder(object, rows, standard_cols)
		if pop_err != nil {
			return pop_err
		}
	}

	return nil
}

func (q *QueryResult) Scan(args ...interface{}) error {
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

//GENERIC QUERYING => Query, Exec + Out, OutMany
func (q *QueryResult) Out(object DatabaseMapped) error {

	if q.Error != nil {
		if q.Rows != nil {
			q.Rows.Close()
		}
		if q.Stmt != nil {
			q.Stmt.Close()
		}

		return q.Error
	}

	meta := GetColumns(object)

	if q.Rows.Next() {
		pop_err := PopulateByName(object, q.Rows, meta)
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

func (q *QueryResult) OutMany(collection interface{}) error {
	if q.Error != nil {
		if q.Rows != nil {
			q.Rows.Close()
		}
		if q.Stmt != nil {
			q.Stmt.Close()
		}

		return q.Error
	}

	collection_value := ReflectValue(collection)
	t := ReflectSliceType(collection)
	slice_t := ReflectType(collection)
	meta := GetColumnsByType(t)

	did_set_rows := false
	for q.Rows.Next() {
		new_obj := MakeNew(t)
		pop_err := PopulateByName(new_obj, q.Rows, meta)
		if pop_err != nil {
			if q.Rows != nil {
				q.Rows.Close()
			}
			if q.Stmt != nil {
				q.Stmt.Close()
			}

			return pop_err
		}
		new_obj_value := ReflectValue(new_obj)
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

//type erasure's bad mmmkay.
func (db_alias *DBConnection) GetAll(collection interface{}) error {
	return db_alias.GetAllInTransaction(collection, nil)
}

func (db_alias *DBConnection) GetAllInTransaction(collection interface{}, tx *sql.Tx) error {

	collection_value := ReflectValue(collection)
	t := ReflectSliceType(collection)
	table_name := TableName(t)
	meta := GetColumnsByType(t)

	column_names := meta.NotReadonly().ColumnNames()

	sql_stmt := fmt.Sprintf("SELECT %s FROM %s", strings.Join(column_names, ","), table_name)

	stmt, statment_err := db_alias.Prepare(sql_stmt, tx)
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
		new_obj := MakeNew(t)
		pop_err := PopulateInOrder(new_obj, rows, meta)
		if pop_err != nil {
			return pop_err
		}
		//USE THE REFLECT LUKE.
		//collection = append(collection, new_obj)

		new_obj_value := ReflectValue(new_obj)
		collection_value.Set(reflect.Append(collection_value, new_obj_value))
	}

	return nil
}

func (db_alias *DBConnection) Create(object DatabaseMapped) error {
	return db_alias.CreateInTransaction(object, nil)
}

func (db_alias *DBConnection) CreateInTransaction(object DatabaseMapped, tx *sql.Tx) error {
	cols := GetColumns(object)
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

	stmt, stmt_err := db_alias.Prepare(sql_stmt, tx)
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

func (db_alias *DBConnection) Update(object DatabaseMapped) error {
	return db_alias.UpdateInTransaction(object, nil)
}

func (db_alias *DBConnection) UpdateInTransaction(object DatabaseMapped, tx *sql.Tx) error {
	table_name := object.TableName()

	cols := GetColumns(object)
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

	stmt, stmt_err := db_alias.Prepare(sql_stmt, tx)
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

func (db_alias *DBConnection) Upsert(object DatabaseMapped) error {
	return db_alias.UpsertInTransaction(object, nil)
}

//TODO: make this database level atomic (read -> update || insert)
func (db_alias *DBConnection) UpsertInTransaction(object DatabaseMapped, tx *sql.Tx) error {
	exists, exists_err := db_alias.ExistsInTransaction(object, tx)
	if exists_err != nil {
		return exists_err
	}

	if exists {
		return db_alias.UpdateInTransaction(object, tx)
	} else {
		return db_alias.CreateInTransaction(object, tx)
	}
}

func (db_alias *DBConnection) Exists(object DatabaseMapped) (bool, error) {
	return db_alias.ExistsInTransaction(object, nil)
}

func (db_alias *DBConnection) ExistsInTransaction(object DatabaseMapped, tx *sql.Tx) (bool, error) {
	table_name := object.TableName()
	cols := GetColumns(object)
	pks := cols.PrimaryKeys()

	if len(pks.Columns) == 0 {
		return false, errors.New("No primary key on object.")
	}
	where_clause := makeWhereClause(pks, 1)
	sql_stmt := fmt.Sprintf("SELECT 1 FROM %s %s", table_name, where_clause)
	stmt, stmt_err := db_alias.Prepare(sql_stmt, tx)
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

func (db_alias *DBConnection) Delete(object DatabaseMapped) error {
	return db_alias.DeleteInTransaction(object, nil)
}

func (db_alias *DBConnection) DeleteInTransaction(object DatabaseMapped, tx *sql.Tx) error {
	table_name := object.TableName()
	cols := GetColumns(object)
	pks := cols.PrimaryKeys()

	if len(pks.Columns) == 0 {
		return errors.New("No primary key on object.")
	}

	where_clause := makeWhereClause(pks, 1)
	sql_stmt := fmt.Sprintf("DELETE FROM %s %s", table_name, where_clause)

	stmt, stmt_err := db_alias.Prepare(sql_stmt, tx)
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

//note object has to be a &!
func (c Column) SetValue(object DatabaseMapped, value interface{}) error {
	obj_value := ReflectValue(object)
	field := obj_value.FieldByName(c.FieldName)
	field_type := field.Type()
	if field.CanSet() {
		value_reflected := ReflectValue(value)
		if value_reflected.IsValid() {
			if c.IsJson {
				typed_value, ok := value_reflected.Interface().(string)
				if ok && len(typed_value) != 0 {
					field_addr := field.Addr().Interface()
					json_err := JsonDeserialize(field_addr, typed_value)
					if json_err != nil {
						return json_err
					}
					field.Set(reflect.ValueOf(field_addr).Elem())
				}
			} else {
				if value_reflected.Type().AssignableTo(field_type) {
					if field.Kind() == reflect.Ptr && value_reflected.CanAddr() {
						field.Set(value_reflected.Addr())
					} else {
						field.Set(value_reflected)
					}
				} else {
					if field.Kind() == reflect.Ptr {
						if value_reflected.CanAddr() {
							if field_type.Elem() == value_reflected.Type() {
								field.Set(value_reflected.Addr())
							} else {
								converted_value := value_reflected.Convert(field_type.Elem())
								if converted_value.CanAddr() {
									field.Set(converted_value.Addr())

								}
							}
						}
					} else {
						converted_value := value_reflected.Convert(field_type)
						field.Set(converted_value)
					}
				}
			}
		}
	} else {
		return errors.New("hit a field we can't set: '" + c.FieldName + "', did you forget to pass the object as a reference?")
	}
	return nil
}

func (c Column) GetValue(object DatabaseMapped) interface{} {
	value := ReflectValue(object)
	value_field := value.Field(c.Index)
	return value_field.Interface()
}

func ReflectValue(obj interface{}) reflect.Value {
	v := reflect.ValueOf(obj)
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return v
}

func ReflectType(obj interface{}) reflect.Type {
	t := reflect.TypeOf(obj)
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Interface {
		t = t.Elem()
	}

	return t
}

func ReflectSliceType(collection interface{}) reflect.Type {
	t := reflect.TypeOf(collection)
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Interface || t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	return t
}

func CreateColumnCollection(columns []Column) ColumnCollection {
	cc := ColumnCollection{Columns: columns}
	lookup := make(map[string]*Column)
	for i := 0; i < len(columns); i++ {
		col := &columns[i]
		lookup[col.ColumnName] = col
	}
	cc.Lookup = lookup
	return cc
}

//things we use as where predicates and can't update
func (cc ColumnCollection) PrimaryKeys() ColumnCollection {
	var pks []Column
	for _, c := range cc.Columns {
		if c.IsPrimaryKey {
			pks = append(pks, c)
		}
	}
	return CreateColumnCollection(pks)
}

//things we can update
func (cc ColumnCollection) NotPrimaryKeys() ColumnCollection {
	var pks []Column
	for _, c := range cc.Columns {
		if !c.IsPrimaryKey {
			pks = append(pks, c)
		}
	}
	return CreateColumnCollection(pks)
}

//things we have to return the id of ...
func (cc ColumnCollection) Serials() ColumnCollection {
	var pks []Column
	for _, c := range cc.Columns {
		if c.IsSerial {
			pks = append(pks, c)
		}
	}
	return CreateColumnCollection(pks)
}

//things we don't have to return the id of ...
func (cc ColumnCollection) NotSerials() ColumnCollection {
	var pks []Column
	for _, c := range cc.Columns {
		if !c.IsSerial {
			pks = append(pks, c)
		}
	}
	return CreateColumnCollection(pks)
}

//a.k.a. not things we insert
func (cc ColumnCollection) ReadOnly() ColumnCollection {
	var pks []Column
	for _, c := range cc.Columns {
		if c.IsReadOnly {
			pks = append(pks, c)
		}
	}
	return CreateColumnCollection(pks)
}

func (cc ColumnCollection) NotReadonly() ColumnCollection {
	var pks []Column
	for _, c := range cc.Columns {
		if !c.IsReadOnly {
			pks = append(pks, c)
		}
	}
	return CreateColumnCollection(pks)
}

func (cc ColumnCollection) ColumnNames() []string {
	var names []string
	for _, c := range cc.Columns {
		names = append(names, c.ColumnName)
	}
	return names
}

func (cc ColumnCollection) ColumnValues(instance interface{}) []interface{} {
	value := ReflectValue(instance)

	var values []interface{}
	for _, c := range cc.Columns {
		value_field := value.FieldByName(c.FieldName)
		if c.IsJson {
			to_serialize := value_field.Interface()
			values = append(values, JsonSerialize(to_serialize))
		} else {
			values = append(values, value_field.Interface())
		}

	}
	return values
}

func (cc ColumnCollection) FirstOrDefault() *Column {
	if len(cc.Columns) > 0 {
		col := cc.Columns[0]
		return &col
	} else {
		return nil
	}
}

func (cc ColumnCollection) ConcatWith(other ColumnCollection) ColumnCollection {
	var total []Column
	for _, c := range cc.Columns {
		total = append(total, c)
	}
	for _, c := range other.Columns {
		total = append(total, c)
	}
	return CreateColumnCollection(total)
}

func makeWhereClause(pks ColumnCollection, start_at int) string {
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

func TableName(t reflect.Type) string {
	return MakeNew(t).TableName()
}

func MakeNew(t reflect.Type) DatabaseMapped {
	new_interface := reflect.New(t).Interface()
	return new_interface.(DatabaseMapped)
}

func MakeSliceOfType(t reflect.Type) interface{} {
	return reflect.New(reflect.SliceOf(t)).Interface()
}

func Populate(object DatabaseMapped, row *sql.Rows) error {
	return PopulateByName(object, row, GetColumns(object))
}

func PopulateByName(object DatabaseMapped, row *sql.Rows, cols ColumnCollection) error {
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

func PopulateInOrder(object DatabaseMapped, row *sql.Rows, cols ColumnCollection) error {
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

func GetColumns(object DatabaseMapped) ColumnCollection {
	return GetColumnsByType(reflect.TypeOf(object))
}

func GetColumnsByType(t reflect.Type) ColumnCollection {
	metaCacheLock.Lock()
	defer metaCacheLock.Unlock()
	if metaCache == nil {
		metaCache = map[reflect.Type]ColumnCollection{}
	}

	if _, ok := metaCache[t]; !ok {
		metaCache[t] = CreateColumnsByType(t)
	}
	return metaCache[t]
}

func CreateColumnsByType(t reflect.Type) ColumnCollection {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	table_name := TableName(t)

	number_of_fields := t.NumField()

	var cols []Column
	for index := 0; index < number_of_fields; index++ {
		field := t.Field(index)
		if !field.Anonymous {
			col := ReadFieldTag(field)
			if col != nil {
				col.Index = index
				col.TableName = table_name
				cols = append(cols, *col)
			}
		}
	}

	return CreateColumnCollection(cols)
}

// reads the contents of a field tag, ex: `json:"foo" db:"bar,isprimarykey,isserial"
func ReadFieldTag(field reflect.StructField) *Column {
	db := field.Tag.Get("db")
	if db != "-" {
		col := Column{}
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

func JsonSerialize(object interface{}) string {
	b, _ := json.Marshal(object)
	return string(b)
}

func JsonDeserialize(object interface{}, body string) error {
	return json.Unmarshal([]byte(body), &object)
}
