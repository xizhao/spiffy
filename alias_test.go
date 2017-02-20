package spiffy

import (
	"testing"

	"github.com/blendlabs/go-assert"
)

func TestAliases(t *testing.T) {
	a := assert.New(t)
	config := NewConnectionFromEnvironment()

	CreateAlias("test", config)

	gotConn := Alias("test")
	a.Equal(config.Username, gotConn.Username, "Alias(name) should return the correct alias.")

	SetDefault(config)
	defaultConn := Default()
	a.NotNil(defaultConn, "DefaultDb() with an alias should return the aliased connection.")
}
