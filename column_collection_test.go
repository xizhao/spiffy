package spiffy

import (
	"testing"

	"github.com/blendlabs/go-assert"
)

type subStruct struct {
	Foo string `json:"foo"`
}

type myStruct struct {
	PrimaryKeyCol     int       `json:"pk" db:"primary_key_column,pk,serial"`
	InferredName      string    `json:"normal"`
	Excluded          string    `json:"-" db:"-"`
	NullableCol       string    `json:"not_nullable" db:"nullable,nullable"`
	InferredWithFlags string    `db:",readonly"`
	BigIntColumn      int64     `db:"big_int"`
	PointerColumn     *int      `db:"pointer_col"`
	JSONColumn        subStruct `db:"json_col,json"`
}

func (m myStruct) TableName() string {
	return "my_struct"
}

func TestGetColumns(t *testing.T) {
	a := assert.New(t)

	emptyColumnCollection := ColumnCollection{}
	firstOrDefaultNil := emptyColumnCollection.FirstOrDefault()
	a.Nil(firstOrDefaultNil)

	obj := myStruct{}
	meta := CachedColumnCollectionFromInstance(obj)

	a.NotNil(meta.Columns())
	a.NotEmpty(meta.Columns())

	a.Equal(7, meta.Len())

	readOnlyColumns := meta.ReadOnly()
	a.Len(readOnlyColumns.Columns(), 1)

	firstOrDefault := meta.FirstOrDefault()
	a.NotNil(firstOrDefault)

	firstCol := meta.FirstOrDefault()
	a.Equal("my_struct", firstCol.TableName)
	a.Equal("PrimaryKeyCol", firstCol.FieldName)
	a.Equal("primary_key_column", firstCol.ColumnName)
	a.True(firstCol.IsPrimaryKey)
	a.True(firstCol.IsSerial)
	a.False(firstCol.IsNullable)
	a.False(firstCol.IsReadOnly)

	secondCol := meta.Columns()[1]
	a.Equal("inferredname", secondCol.ColumnName)
	a.False(secondCol.IsPrimaryKey)
	a.False(secondCol.IsSerial)
	a.False(secondCol.IsNullable)
	a.False(secondCol.IsReadOnly)

	thirdCol := meta.Columns()[2]
	a.Equal("nullable", thirdCol.ColumnName)
	a.False(thirdCol.IsPrimaryKey)
	a.False(thirdCol.IsSerial)
	a.True(thirdCol.IsNullable)
	a.False(thirdCol.IsReadOnly)

	fourthCol := meta.Columns()[3]
	a.Equal("inferredwithflags", fourthCol.ColumnName)
	a.False(fourthCol.IsPrimaryKey)
	a.False(fourthCol.IsSerial)
	a.False(fourthCol.IsNullable)
	a.True(fourthCol.IsReadOnly)
}

func TestColumnCollectionCopy(t *testing.T) {
	assert := assert.New(t)

	obj := myStruct{}
	meta := CachedColumnCollectionFromInstance(obj)
	newMeta := meta.Copy()
	assert.False(meta == newMeta, "These pointers should not be the same.")
	newMeta.columnPrefix = "foo_"
	assert.NotEqual(meta.columnPrefix, newMeta.columnPrefix)
}

func TestColumnCollectionWithColumnPrefix(t *testing.T) {
	assert := assert.New(t)

	obj := myStruct{}
	meta := CachedColumnCollectionFromInstance(obj)
	newMeta := meta.WithColumnPrefix("foo_")
	assert.Equal("foo_", newMeta.columnPrefix)
	assert.False(meta == newMeta, "These pointers should not be the same.")
	assert.NotEqual(meta.columnPrefix, newMeta.columnPrefix)
}