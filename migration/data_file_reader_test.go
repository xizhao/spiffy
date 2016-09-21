package migration

import (
	"testing"

	assert "github.com/blendlabs/go-assert"
)

func TestDataFileReaderExtractDataLine(t *testing.T) {
	assert := assert.New(t)

	dfr := &DataFileReader{}

	components := dfr.extractDataLine(`1	one	\N	['a','b','c']	a`)
	assert.Equal("1", components[0])
	assert.Equal("one", components[1])
	assert.Nil(components[2])
	assert.Equal("['a','b','c']", components[3])
	assert.Equal("a", components[4])
}
