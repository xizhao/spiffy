package migration

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	assert "github.com/blendlabs/go-assert"
)

func TestDataFileReaderReadLineSingleLine(t *testing.T) {
	assert := assert.New(t)

	fileBuffer := bytes.NewReader([]byte(`this is a test line`))
	dfr := &DataFileReader{}

	var cursor int64
	var readBuffer = make([]byte, 32)
	var readErr error
	var lineBuffer = bytes.NewBuffer([]byte{})

	cursor, readErr = dfr.readLine(fileBuffer, cursor, readBuffer, lineBuffer)
	assert.NotNil(readErr)
	assert.Equal(io.EOF, readErr)
	assert.Equal(19, cursor)
	assert.Equal(19, lineBuffer.Len())
	assert.Equal("this is a test line", lineBuffer.String())
}

func TestDataFileReaderReadLine(t *testing.T) {
	assert := assert.New(t)

	fileBuffer := bytes.NewReader([]byte(`this is a test line
this is another test line
this is 3rd test line
this is a 4th test line

this is after a blank line
`))
	dfr := &DataFileReader{}

	var cursor int64
	var readBuffer = make([]byte, 32)
	var readErr error
	var lineBuffer = bytes.NewBuffer([]byte{})

	cursor, readErr = dfr.readLine(fileBuffer, cursor, readBuffer, lineBuffer)
	assert.Nil(readErr)
	assert.Equal(20, cursor)
	assert.Equal(19, lineBuffer.Len())
	assert.Equal("this is a test line", lineBuffer.String())

	lineBuffer.Reset()
	cursor, readErr = dfr.readLine(fileBuffer, cursor, readBuffer, lineBuffer)
	assert.Nil(readErr)
	assert.Equal(46, cursor)
	assert.Equal(25, lineBuffer.Len())
	assert.Equal("this is another test line", lineBuffer.String())

	lineBuffer.Reset()
	cursor, readErr = dfr.readLine(fileBuffer, cursor, readBuffer, lineBuffer)
	assert.Nil(readErr)
	assert.Equal(68, cursor)
	assert.Equal(21, lineBuffer.Len())
	assert.Equal("this is 3rd test line", lineBuffer.String())

	lineBuffer.Reset()
	cursor, readErr = dfr.readLine(fileBuffer, cursor, readBuffer, lineBuffer)
	assert.Nil(readErr)
	assert.Equal(92, cursor)
	assert.Equal(23, lineBuffer.Len())
	assert.Equal("this is a 4th test line", lineBuffer.String())

	lineBuffer.Reset()
	cursor, readErr = dfr.readLine(fileBuffer, cursor, readBuffer, lineBuffer)
	assert.Nil(readErr, fmt.Sprintf("Total Buffer Len: %d, cusor: %d", fileBuffer.Len(), cursor))
	assert.Equal(93, cursor)
	assert.Equal(0, lineBuffer.Len())
	assert.Empty(lineBuffer.String())

	lineBuffer.Reset()
	cursor, readErr = dfr.readLine(fileBuffer, cursor, readBuffer, lineBuffer)
	assert.NotNil(readErr)
	assert.Equal(io.EOF, readErr)
	assert.Equal(120, cursor)
	assert.Equal(26, lineBuffer.Len())
	assert.Equal("this is after a blank line", lineBuffer.String())
}

func TestDataFileReaderReadTabLine(t *testing.T) {
	assert := assert.New(t)

	fileBuffer := bytes.NewReader([]byte(`hello world	123	\N	testing
1	2	3	4	5	6	7	8	9	10
this is a line that ends in a tab	`))

	dfr := &DataFileReader{}

	var cursor int64
	var readBuffer = make([]byte, 32)
	var readErr error
	var lineBuffer = bytes.NewBuffer([]byte{})
	var pieces []interface{}

	pieces, cursor, readErr = dfr.readTabLine(fileBuffer, cursor, readBuffer, lineBuffer)
	assert.Nil(readErr)
	assert.Len(pieces, 4, fmt.Sprintf("%#v", pieces))
	assert.Equal("hello world", pieces[0])
	assert.Equal("123", pieces[1])
	assert.Nil(pieces[2])

	lineBuffer.Reset()
	pieces, cursor, readErr = dfr.readTabLine(fileBuffer, cursor, readBuffer, lineBuffer)
	assert.Nil(readErr)
	assert.Len(pieces, 10)
	assert.Equal("1", pieces[0])
	assert.Equal("10", pieces[9])

	lineBuffer.Reset()
	pieces, cursor, readErr = dfr.readTabLine(fileBuffer, cursor, readBuffer, lineBuffer)
	assert.NotNil(readErr)
	assert.Equal(io.EOF, readErr)
	assert.Len(pieces, 1)
	assert.Equal("this is a line that ends in a tab", pieces[0])
}
