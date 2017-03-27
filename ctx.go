package spiffy

import (
	"database/sql"

	exception "github.com/blendlabs/go-exception"
)

// NewCtx returns a new ctx.
func NewCtx() *Ctx {
	return &Ctx{}
}

// Ctx represents a connection context.
// It rolls both the underlying connection and an optional tx into one struct.
// The motivation here is so that if you have datamanager functions they can be
// used across databases, and don't assume internally which db they talk to.
type Ctx struct {
	conn *Connection
	tx   *sql.Tx
	err  error
}

// WithConn sets the connection for the context.
func (c *Ctx) WithConn(conn *Connection) *Ctx {
	c.conn = conn
	return c
}

// Conn returns the underlying connection for the context.
func (c *Ctx) Conn() *Connection {
	return c.conn
}

// InTx isolates a context to a transaction.
// The order precedence of the three main transaction sources are as follows:
// - InTx(...) transaction arguments will be used above everything else
// - an existing transaction on the context (i.e. if you call `.InTx().InTx()`)
// - beginning a new transaction with the connection
func (c *Ctx) InTx(txs ...*sql.Tx) *Ctx {
	if len(txs) > 0 {
		c.tx = txs[0]
		return c
	}
	if c.tx != nil {
		return c
	}
	if c.conn == nil {
		c.err = exception.Newf(connectionErrorMessage)
		return c
	}
	c.tx, c.err = c.conn.Begin()
	return c
}

// Tx returns the transction for the context.
func (c *Ctx) Tx() *sql.Tx {
	return c.tx
}

// Commit calls `Commit()` on the underlying transaction.
func (c *Ctx) Commit() error {
	if c.tx == nil {
		return nil
	}
	return c.tx.Commit()
}

// Rollback calls `Rollback()` on the underlying transaction.
func (c *Ctx) Rollback() error {
	if c.tx == nil {
		return nil
	}
	return c.tx.Rollback()
}

// Err returns the carried error.
func (c *Ctx) Err() error {
	return c.err
}

// Invoke starts a new invocation.
func (c *Ctx) Invoke() *Invocation {
	return &Invocation{ctx: c, err: c.err}
}
