package otel

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/varunreddy-egov/tracer/pkg/db"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// OtelDBTracer implements the DBTracer interface using OpenTelemetry
type OtelDBTracer struct {
	tracer *Tracer
}

// NewOtelDBTracer creates a new OpenTelemetry database tracer
func NewOtelDBTracer(tracer *Tracer) *OtelDBTracer {
	return &OtelDBTracer{
		tracer: tracer,
	}
}

// TraceQuery traces a database query operation
func (dt *OtelDBTracer) TraceQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	// Set database operation attributes
	span.SetAttributes(
		attribute.String("db.operation", "query"),
		attribute.String("db.statement", query),
		attribute.String("db.args", db.FormatArgs(args)),
		attribute.Float64("db.duration_ms", float64(duration.Nanoseconds())/1e6),
		attribute.Int("db.args_count", len(args)),
	)

	// Add event for the query
	span.AddEvent("db.query", trace.WithAttributes(
		attribute.String("query", query),
		attribute.Float64("duration_ms", float64(duration.Nanoseconds())/1e6),
	))

	// Record error if present
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("Query failed: %v", err))
		dt.tracer.AddAttribute(ctx, "db.error", err.Error())
	} else {
		span.SetStatus(codes.Ok, "Query executed successfully")
	}

	// Add performance indicators
	if duration > 100*time.Millisecond {
		dt.tracer.AddAttribute(ctx, "db.slow_query", true)
		span.AddEvent("slow_query_detected", trace.WithAttributes(
			attribute.Float64("duration_ms", float64(duration.Nanoseconds())/1e6),
		))
	}
}

// TraceExec traces a database execution operation (INSERT, UPDATE, DELETE)
func (dt *OtelDBTracer) TraceExec(ctx context.Context, query string, args []interface{}, duration time.Duration, rowsAffected int64, err error) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	// Set database operation attributes
	span.SetAttributes(
		attribute.String("db.operation", "exec"),
		attribute.String("db.statement", query),
		attribute.String("db.args", db.FormatArgs(args)),
		attribute.Float64("db.duration_ms", float64(duration.Nanoseconds())/1e6),
		attribute.Int("db.args_count", len(args)),
		attribute.Int64("db.rows_affected", rowsAffected),
	)

	// Add event for the execution
	span.AddEvent("db.exec", trace.WithAttributes(
		attribute.String("query", query),
		attribute.Float64("duration_ms", float64(duration.Nanoseconds())/1e6),
		attribute.Int64("rows_affected", rowsAffected),
	))

	// Record error if present
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("Execution failed: %v", err))
		dt.tracer.AddAttribute(ctx, "db.error", err.Error())
	} else {
		span.SetStatus(codes.Ok, "Execution completed successfully")
	}

	// Add performance indicators
	if duration > 100*time.Millisecond {
		dt.tracer.AddAttribute(ctx, "db.slow_query", true)
		span.AddEvent("slow_execution_detected", trace.WithAttributes(
			attribute.Float64("duration_ms", float64(duration.Nanoseconds())/1e6),
		))
	}
}

// TraceTx traces database transaction operations
func (dt *OtelDBTracer) TraceTx(ctx context.Context, operation string, duration time.Duration, err error) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	// Set transaction attributes
	span.SetAttributes(
		attribute.String("db.operation", "transaction"),
		attribute.String("db.transaction.operation", operation),
		attribute.Float64("db.duration_ms", float64(duration.Nanoseconds())/1e6),
	)

	// Add event for the transaction operation
	span.AddEvent(fmt.Sprintf("db.tx.%s", operation), trace.WithAttributes(
		attribute.String("operation", operation),
		attribute.Float64("duration_ms", float64(duration.Nanoseconds())/1e6),
	))

	// Record error if present
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("Transaction %s failed: %v", operation, err))
		dt.tracer.AddAttribute(ctx, "db.transaction.error", err.Error())
	} else {
		span.SetStatus(codes.Ok, fmt.Sprintf("Transaction %s completed successfully", operation))
	}
}

// CreateTracedDB creates a traced database connection using the global tracer
func CreateTracedDB(sqlDB interface{}, dbType string) *db.TracedDB {
	tracer := GetGlobalTracer()
	dbTracer := NewOtelDBTracer(tracer)

	// Type assertion to get *sql.DB
	if sqlDatabase, ok := sqlDB.(*sql.DB); ok {
		return db.NewTracedDB(sqlDatabase, dbTracer)
	}

	// If not *sql.DB, return nil (could be enhanced to support other DB types)
	return nil
}

// WithDBSpan executes a function within a database-specific span
func (t *Tracer) WithDBSpan(ctx context.Context, operation string, tableName string, fn func(ctx context.Context) error) error {
	spanName := fmt.Sprintf("db.%s", operation)
	if tableName != "" {
		spanName = fmt.Sprintf("db.%s.%s", operation, tableName)
	}

	return t.WithSpanSimple(ctx, spanName, func(ctx context.Context) error {
		// Add database-specific attributes
		t.AddAttribute(ctx, "db.operation.type", operation)
		if tableName != "" {
			t.AddAttribute(ctx, "db.table", tableName)
		}

		return fn(ctx)
	})
}
