package pgxotel

import (
	"context"
	"database/sql"
	"errors"
	"regexp"

	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	otel "go.opentelemetry.io/otel"
	attribute "go.opentelemetry.io/otel/attribute"
	codes "go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	trace "go.opentelemetry.io/otel/trace"
)

var (
	_ pgx.QueryTracer    = (*Tracer)(nil)
	_ pgx.BatchTracer    = (*Tracer)(nil)
	_ pgx.ConnectTracer  = (*Tracer)(nil)
	_ pgx.PrepareTracer  = (*Tracer)(nil)
	_ pgx.CopyFromTracer = (*Tracer)(nil)
)

// Tracer is a wrapper around the pgx tracer interfaces which instrument queries.
type Tracer struct {
	// tracer represents the tracer
	tracer trace.Tracer
}

// NewTracer creates a new tracer
func NewTracer(name string, options ...trace.TracerOption) *Tracer {
	return &Tracer{
		tracer: otel.GetTracerProvider().Tracer(name, options...),
	}
}

// TraceConnectStart implements pgx.ConnectTracer.
func (t *Tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := t.options(data.ConnConfig)
	// prepare the span
	ctx, _ = t.tracer.Start(ctx, "connect", opts...)
	// done!
	return ctx
}

// TraceConnectEnd implements pgx.ConnectTracer.
func (t *Tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	span := trace.SpanFromContext(ctx)
	defer span.End()
	// log the error
	t.error(span, data.Err)
}

// TracePrepareStart implements pgx.PrepareTracer.
func (t *Tracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := []trace.SpanStartOption{}
	opts = append(opts, t.options(conn.Config())...)
	opts = append(opts, t.query(data.SQL)...)

	name := t.span("prepare", data.SQL)
	// prepare the context
	ctx, _ = t.tracer.Start(ctx, name, opts...)
	// done!
	return ctx
}

// TracePrepareEnd implements pgx.PrepareTracer.
func (t *Tracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	span := trace.SpanFromContext(ctx)
	defer span.End()
	// log the error
	t.error(span, data.Err)
}

// TraceQueryStart implements pgx.QueryTracer.
func (t *Tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := []trace.SpanStartOption{}
	opts = append(opts, t.options(conn.Config())...)
	opts = append(opts, t.query(data.SQL)...)

	name := t.span("query", data.SQL)
	// prepare the context
	ctx, _ = t.tracer.Start(ctx, name, opts...)
	// done!
	return ctx
}

// TraceQueryEnd implements pgx.QueryTracer.
func (t *Tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	defer span.End()

	if data.Err != nil {
		span.SetAttributes(DBRowsAffected(data.CommandTag))
	}

	// log the error
	t.error(span, data.Err)
}

// TraceCopyFromStart implements pgx.CopyFromTracer.
func (t *Tracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := t.options(conn.Config())
	// prepare the options
	opts = append(opts,
		trace.WithAttributes(
			semconv.DBSQLTable(data.TableName.Sanitize()),
		),
	)

	// prepare the context
	ctx, _ = t.tracer.Start(ctx, "copy_from", opts...)
	// done!
	return ctx
}

// TraceCopyFromEnd implements pgx.CopyFromTracer.
func (t *Tracer) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
	span := trace.SpanFromContext(ctx)
	defer span.End()

	if data.Err != nil {
		span.SetAttributes(DBRowsAffected(data.CommandTag))
	}

	// log the error
	t.error(span, data.Err)
}

// TraceBatchStart implements pgx.BatchTracer.
func (t *Tracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := t.options(conn.Config())
	// prepare the options
	opts = append(opts,
		trace.WithAttributes(
			DBOperationCount(data.Batch),
		),
	)

	// prepare the context
	ctx, _ = t.tracer.Start(ctx, "batch", opts...)
	// done!
	return ctx
}

// TraceBatchQuery implements pgx.BatchTracer.
func (t *Tracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	opts := []trace.SpanStartOption{}
	opts = append(opts, t.options(conn.Config())...)
	opts = append(opts, t.query(data.SQL)...)
	opts = append(opts, trace.WithAttributes(
		DBRowsAffected(data.CommandTag),
	))

	// prepare the context
	_, span := t.tracer.Start(ctx, "batch_query", opts...)
	defer span.End()
	// done!
	t.error(span, data.Err)
}

// TraceBatchEnd implements pgx.BatchTracer.
func (t *Tracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	span := trace.SpanFromContext(ctx)
	defer span.End()

	// log the error
	t.error(span, data.Err)
}

func (t *Tracer) options(config *pgx.ConnConfig) []trace.SpanStartOption {
	return []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.NetPeerName(config.Host),
			semconv.NetPeerPort(int(config.Port)),
			// database attributes
			semconv.DBSystemPostgreSQL,
			semconv.DBUser(config.User),
			semconv.DBName(config.Database),
		),
	}
}

func (q *Tracer) span(prefix, command string) string {
	if name := q.name(command); name != "unknown" {
		command = name
	}

	return prefix + " " + command
}

var pattern = regexp.MustCompile(`^--\s+name:\s+(\w+)`)

func (q *Tracer) name(v string) string {
	if match := pattern.FindStringSubmatch(v); len(match) == 2 {
		return match[1]
	}

	return "unknown"
}

func (q *Tracer) query(command string) []trace.SpanStartOption {
	name := q.name(command)

	return []trace.SpanStartOption{
		trace.WithAttributes(
			semconv.DBOperation(name),
			semconv.DBStatement(command),
		),
	}
}

func (t *Tracer) error(span trace.Span, err error) {
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			if !errors.Is(err, pgx.ErrNoRows) {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())

				var pgerr *pgconn.PgError

				if errors.As(err, &pgerr) {
					span.SetAttributes(DBErrorCode(pgerr))
					span.SetAttributes(DBErrorMessage(pgerr))
				}
			}
		}
	}
}

func DBErrorCode(err *pgconn.PgError) attribute.KeyValue {
	const key = attribute.Key("db.error_code")
	return key.String(err.Code)
}

func DBErrorMessage(err *pgconn.PgError) attribute.KeyValue {
	const key = attribute.Key("db.error_message")
	return key.String(err.Message)
}

func DBRowsAffected(tag pgconn.CommandTag) attribute.KeyValue {
	const key = attribute.Key("db.rows_affected")
	return key.Int64(tag.RowsAffected())
}

func DBOperationCount(batch *pgx.Batch) attribute.KeyValue {
	const key = attribute.Key("db.operation_count")
	return key.Int(batch.Len())
}
