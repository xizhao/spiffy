package spiffy

import (
	"fmt"
	"testing"

	assert "github.com/blendlabs/go-assert"
)

func TestCtxInTxUsesArguments(t *testing.T) {
	assert := assert.New(t)
	tx, err := DB().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	withTx := NewCtx().InTx(tx)
	assert.NotNil(withTx.tx)
}

func TestCtxInTxReturnsAnExistingTransaction(t *testing.T) {
	assert := assert.New(t)
	tx, err := DB().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	withTx := NewCtx().InTx(tx).InTx()
	assert.NotNil(withTx.tx)
	assert.Equal(tx, withTx.Tx())
}

func TestCtxInTx(t *testing.T) {
	assert := assert.New(t)

	withTx := NewCtx().WithConn(DB()).InTx()
	defer withTx.Rollback()
	assert.NotNil(withTx.tx)
}

func TestCtxInTxWithoutConnection(t *testing.T) {
	assert := assert.New(t)

	withTx := NewCtx().InTx()
	assert.Nil(withTx.tx)
	assert.NotNil(withTx.err)
}

func TestCtxInvoke(t *testing.T) {
	assert := assert.New(t)

	inv := NewCtx().WithConn(DB()).Invoke()
	assert.Nil(inv.check())
}

func TestCtxInvokeError(t *testing.T) {
	assert := assert.New(t)

	inv := NewCtx().Invoke()
	assert.NotNil(inv.check(), "should fail the connection not nil check")
}

func TestCtxInvokeCarriesError(t *testing.T) {
	assert := assert.New(t)

	ctx := NewCtx().WithConn(DB())
	ctx.err = fmt.Errorf("test error")
	inv := ctx.Invoke()
	assert.NotNil(inv.check())
}
