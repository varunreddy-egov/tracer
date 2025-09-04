package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// TracedDB wraps a database connection with tracing capabilities
type TracedDB struct {
	db     *sql.DB
	tracer DBTracer
}

// DBTracer interface for database tracing
type DBTracer interface {
	TraceQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error)
	TraceExec(ctx context.Context, query string, args []interface{}, duration time.Duration, rowsAffected int64, err error)
	TraceTx(ctx context.Context, operation string, duration time.Duration, err error)
}

// NewTracedDB creates a new traced database connection
func NewTracedDB(db *sql.DB, tracer DBTracer) *TracedDB {
	return &TracedDB{
		db:     db,
		tracer: tracer,
	}
}

// Query executes a query with tracing
func (tdb *TracedDB) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := tdb.db.QueryContext(ctx, query, args...)
	duration := time.Since(start)
	
	tdb.tracer.TraceQuery(ctx, query, args, duration, err)
	return rows, err
}

// QueryRow executes a query that returns a single row with tracing
func (tdb *TracedDB) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	start := time.Now()
	row := tdb.db.QueryRowContext(ctx, query, args...)
	duration := time.Since(start)
	
	// For QueryRow, we can't know the error until Scan is called
	// So we trace with nil error for now
	tdb.tracer.TraceQuery(ctx, query, args, duration, nil)
	return row
}

// Exec executes a query without returning rows with tracing
func (tdb *TracedDB) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	result, err := tdb.db.ExecContext(ctx, query, args...)
	duration := time.Since(start)
	
	var rowsAffected int64
	if result != nil && err == nil {
		rowsAffected, _ = result.RowsAffected()
	}
	
	tdb.tracer.TraceExec(ctx, query, args, duration, rowsAffected, err)
	return result, err
}

// Begin starts a transaction with tracing
func (tdb *TracedDB) Begin(ctx context.Context) (*TracedTx, error) {
	start := time.Now()
	tx, err := tdb.db.BeginTx(ctx, nil)
	duration := time.Since(start)
	
	tdb.tracer.TraceTx(ctx, "BEGIN", duration, err)
	
	if err != nil {
		return nil, err
	}
	
	return &TracedTx{
		tx:     tx,
		tracer: tdb.tracer,
	}, nil
}

// Close closes the database connection
func (tdb *TracedDB) Close() error {
	return tdb.db.Close()
}

// Ping verifies a connection to the database
func (tdb *TracedDB) Ping(ctx context.Context) error {
	start := time.Now()
	err := tdb.db.PingContext(ctx)
	duration := time.Since(start)
	
	tdb.tracer.TraceQuery(ctx, "PING", nil, duration, err)
	return err
}

// TracedTx wraps a database transaction with tracing
type TracedTx struct {
	tx     *sql.Tx
	tracer DBTracer
}

// Query executes a query within the transaction with tracing
func (ttx *TracedTx) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := ttx.tx.QueryContext(ctx, query, args...)
	duration := time.Since(start)
	
	ttx.tracer.TraceQuery(ctx, query, args, duration, err)
	return rows, err
}

// QueryRow executes a query that returns a single row within the transaction
func (ttx *TracedTx) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	start := time.Now()
	row := ttx.tx.QueryRowContext(ctx, query, args...)
	duration := time.Since(start)
	
	ttx.tracer.TraceQuery(ctx, query, args, duration, nil)
	return row
}

// Exec executes a query without returning rows within the transaction
func (ttx *TracedTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	result, err := ttx.tx.ExecContext(ctx, query, args...)
	duration := time.Since(start)
	
	var rowsAffected int64
	if result != nil && err == nil {
		rowsAffected, _ = result.RowsAffected()
	}
	
	ttx.tracer.TraceExec(ctx, query, args, duration, rowsAffected, err)
	return result, err
}

// Commit commits the transaction with tracing
func (ttx *TracedTx) Commit(ctx context.Context) error {
	start := time.Now()
	err := ttx.tx.Commit()
	duration := time.Since(start)
	
	ttx.tracer.TraceTx(ctx, "COMMIT", duration, err)
	return err
}

// Rollback rolls back the transaction with tracing
func (ttx *TracedTx) Rollback(ctx context.Context) error {
	start := time.Now()
	err := ttx.tx.Rollback()
	duration := time.Since(start)
	
	ttx.tracer.TraceTx(ctx, "ROLLBACK", duration, err)
	return err
}

// Helper function to format SQL arguments for logging
func FormatArgs(args []interface{}) string {
	if len(args) == 0 {
		return "[]"
	}
	
	result := "["
	for i, arg := range args {
		if i > 0 {
			result += ", "
		}
		
		switch v := arg.(type) {
		case string:
			result += fmt.Sprintf("'%s'", v)
		case nil:
			result += "NULL"
		default:
			result += fmt.Sprintf("%v", v)
		}
	}
	result += "]"
	return result
}
