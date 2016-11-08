package spiffy

import (
	"testing"

	"github.com/blendlabs/go-assert"
)

func TestMakeCsvTokens(t *testing.T) {
	a := assert.New(t)

	one := ParamTokensCSV(1)
	two := ParamTokensCSV(2)
	three := ParamTokensCSV(3)

	a.Equal("$1", one)
	a.Equal("$1,$2", two)
	a.Equal("$1,$2,$3", three)
}

func TestReflectSliceType(t *testing.T) {
	assert := assert.New(t)

	objects := []benchObj{
		{}, {}, {},
	}

	ot := reflectSliceType(objects)
	assert.Equal("benchObj", ot.Name())
}

func TestMakeSliceOfType(t *testing.T) {
	a := assert.New(t)
	tx, txErr := DefaultDb().Begin()
	a.Nil(txErr)
	defer func() {
		a.Nil(tx.Rollback())
	}()

	seedErr := seedObjects(10, tx)
	a.Nil(seedErr)

	myType := reflectType(benchObj{})
	sliceOfT, castOk := makeSliceOfType(myType).(*[]benchObj)
	a.True(castOk)

	allErr := DefaultDb().GetAllInTx(sliceOfT, tx)
	a.Nil(allErr)
	a.NotEmpty(*sliceOfT)
}

func TestHasPrefixCaseInsensitive(t *testing.T) {
	assert := assert.New(t)

	assert.True(HasPrefixCaseInsensitive("hello world!", "hello"))
	assert.True(HasPrefixCaseInsensitive("hello world", "hello world"))
	assert.True(HasPrefixCaseInsensitive("HELLO world", "hello"))
	assert.True(HasPrefixCaseInsensitive("hello world", "HELLO"))
	assert.True(HasPrefixCaseInsensitive("hello world", "h"))

	assert.False(HasPrefixCaseInsensitive("hello world", "butters"))
	assert.False(HasPrefixCaseInsensitive("hello world", "hello world boy is this long"))
	assert.False(HasPrefixCaseInsensitive("hello world", "world")) //this would pass suffix
}

func TestHasSuffixCaseInsensitive(t *testing.T) {
	assert := assert.New(t)

	assert.True(HasSuffixCaseInsensitive("hello world!", "world!"))
	assert.True(HasSuffixCaseInsensitive("hello world", "d"))
	assert.True(HasSuffixCaseInsensitive("hello world", "hello world"))

	assert.True(HasSuffixCaseInsensitive("hello WORLD", "world"))
	assert.True(HasSuffixCaseInsensitive("hello world", "WORLD"))

	assert.False(HasSuffixCaseInsensitive("hello world", "hello hello world"))
	assert.False(HasSuffixCaseInsensitive("hello world", "foobar"))
	assert.False(HasSuffixCaseInsensitive("hello world", "hello")) //this would pass prefix
}

func TestCaseInsensitiveEquals(t *testing.T) {
	assert := assert.New(t)
	assert.True(CaseInsensitiveEquals("foo", "FOO"))
	assert.True(CaseInsensitiveEquals("foo123", "FOO123"))
	assert.True(CaseInsensitiveEquals("!foo123", "!foo123"))
	assert.False(CaseInsensitiveEquals("foo", "bar"))
}
