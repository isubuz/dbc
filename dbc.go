package dbc

import (
	"database/sql"
	"errors"
	"fmt"
)

// DB error variables
var (
	ErrDbInsert   = errors.New("SQL insert failed")
	ErrDbSelect   = errors.New("SQL select failed")
	ErrDbUpdate   = errors.New("SQL update failed")
	ErrStmtCreate = errors.New("SQL create statement failed")
	ErrStmtClose  = errors.New("SQL close statement failed")
	ErrStmtExec   = errors.New("SQL execute statement failed")
	ErrTxnBegin   = errors.New("SQL transaction begin failed")
	ErrTxnCommit  = errors.New("SQL transaction commit failed")
)

// Handle defines the interface for the handle which performs the database
// operations.
// A handle can be a database connection or a transaction.
type Handle interface {
	Exec(string, ...interface{}) (sql.Result, error)
	Prepare(string) (Statement, error)
	// Query(string, ...interface{}) Rows
	QueryRow(string, ...interface{}) Row
}

// Row foo
type Row interface {
	Scan(...interface{}) error
}

// Rows foo
type Rows interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(...interface{}) error
}

// Statement foo
type Statement interface {
	Close() error
	Exec(...interface{}) (sql.Result, error)
	// Query(...interface{}) Rows
	QueryRow(...interface{}) Row
}

// Query provides a consise way of representing a SQL query consisting of the
// query and the parameterized arguments.
type Query struct {
	Q    string
	Args []interface{}
}

// NewQuery foo
func NewQuery(q string, args ...interface{}) Query {
	return Query{q, args}
}

func (q Query) String() string {
	return fmt.Sprintf("[%s, %v]", q.Q, q.Args)
}

// DbRow represents a database row.
// It implements the `dbc.Row` interface and wraps row operations using a
// `sql.Row` type.
type DbRow struct {
	R *sql.Row
}

// Scan wraps the call to `sql.Row.Scan()`.
func (r *DbRow) Scan(args ...interface{}) error {
	return r.R.Scan(args)
}

// DbRowV represents the column values in a single row.
type DbRowV struct {
	V []interface{}
}

// DbStmt represents a prepared statement.
// It implements the `dbc.Stmt` interface and wraps statement operations using
// a `sql.Stmt` type.
type DbStmt struct {
	S *sql.Stmt
}

// Close wraps the call to `sql.Stmt.Close()`.
func (st *DbStmt) Close() error {
	return st.S.Close()
}

// Exec wraps the call to `sql.Stmt.Exec()`.
func (st *DbStmt) Exec(args ...interface{}) (sql.Result, error) {
	return st.S.Exec(args)
}

// QueryRow wraps the call to `sql.Stmt.QueryRow` to return a database row.
func (st *DbStmt) QueryRow(args ...interface{}) Row {
	return &DbRow{st.S.QueryRow(args)}
}

// BatchInsert inserts one or more rows.
func (st *DbStmt) BatchInsert(rows []DbRowV) error {
	for _, r := range rows {
		if _, err := st.Exec(r.V...); err != nil {
			return err
		}
	}

	// Next call to `Exec` flushes the writes.
	if _, err := st.Exec(); err != nil {
		return err
	}

	return nil
}

// TxHandle represents the transaction handle.
// It implements the `dbc.Handle` interface and wraps transaction operations
// using a `sql.Tx` type.
type TxHandle struct {
	T *sql.Tx
}

// Exec wraps the call to `sql.Tx.Exec()`.
func (tx TxHandle) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.T.Exec(query, args...)
}

// Prepare wraps the call to `sql.Tx.Prepare()` and returns a prepared statement.
func (tx *TxHandle) Prepare(query string) (Statement, error) {
	s, err := tx.T.Prepare(query)
	return &DbStmt{s}, err
}

// QueryRow wraps the call to `sql.Tx.QueryRow()` and returns a database row.
func (tx *TxHandle) QueryRow(query string, args ...interface{}) Row {
	return &DbRow{tx.T.QueryRow(query, args)}
}

// Commit wraps the call to `sql.Tx.Commit()`.
func (tx *TxHandle) Commit() error {
	return tx.T.Commit()
}

// Rollback wraps the call to `sql.Tx.Rollback()`.
func (tx *TxHandle) Rollback() error {
	return tx.T.Rollback()
}
