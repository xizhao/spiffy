package spiffy

import (
	"testing"

	"fmt"

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

func TestCtxCheck(t *testing.T) {
	assert := assert.New(t)

	connEmpty := &Ctx{}
	err := connEmpty.check()
	assert.NotNil(err)
	assert.Equal(connectionErrorMessage, fmt.Sprintf("%v", err))

	ctxWithErr := &Ctx{conn: DB(), err: fmt.Errorf("this is a test")}
	err = ctxWithErr.check()
	assert.NotNil(err)
	assert.Equal("this is a test", fmt.Sprintf("%v", err))

	ctx := &Ctx{conn: DB()}
	assert.Nil(ctx.check())

	assert.NotNil(ctxWithErr.Exec("wont run"))
	assert.NotNil(ctxWithErr.Query("wont run").Out(nil))
	assert.NotNil(ctxWithErr.Get(nil))
	assert.NotNil(ctxWithErr.GetAll(nil))
	assert.NotNil(ctxWithErr.Create(nil))
	assert.NotNil(ctxWithErr.CreateIfNotExists(nil))
	assert.NotNil(ctxWithErr.CreateMany(nil))
	assert.NotNil(ctxWithErr.Update(nil))
	assert.NotNil(ctxWithErr.Exists(nil))
	assert.NotNil(ctxWithErr.Delete(nil))
	assert.NotNil(ctxWithErr.Upsert(nil))
}
