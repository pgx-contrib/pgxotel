package pgxotel

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"

	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	otel "go.opentelemetry.io/otel"
	attribute "go.opentelemetry.io/otel/attribute"
	codes "go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	trace "go.opentelemetry.io/otel/trace"
)

var (
	_ pgx.QueryTracer    = (*QueryTracer)(nil)
	_ pgx.BatchTracer    = (*QueryTracer)(nil)
	_ pgx.ConnectTracer  = (*QueryTracer)(nil)
	_ pgx.PrepareTracer  = (*QueryTracer)(nil)
	_ pgx.CopyFromTracer = (*QueryTracer)(nil)
)

// QueryTracer is a wrapper around the pgx tracer interfaces which instrument queries.
type QueryTracer struct {
	// Name of the tracer
	Name string
	// Options to provide to the tracer
	Options []trace.TracerOption
}

// TraceConnectStart implements pgx.ConnectTracer.
func (t *QueryTracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	// attributes
	attrs := []attribute.KeyValue{}
	attrs = append(attrs, t.config(data.ConnConfig)...)
	// prepare the span
	ctx, span := t.start(ctx, "Connect", attrs)
	span.AddEvent("ConnectStart")
	// done!
	return ctx
}

// TraceConnectEnd implements pgx.ConnectTracer.
func (t *QueryTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("ConnectEnd")

	attrs := []attribute.KeyValue{}
	// done
	t.stop(span, data.Err, attrs)
}

// TracePrepareStart implements pgx.PrepareTracer.
func (t *QueryTracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	attrs := []attribute.KeyValue{}
	attrs = append(attrs, t.config(conn.Config())...)
	attrs = append(attrs, t.statement(data.SQL))

	// prepare the context
	ctx, span := t.start(ctx, data.SQL, attrs)
	span.AddEvent("PrepareStart")
	// done!
	return ctx
}

// TracePrepareEnd implements pgx.PrepareTracer.
func (t *QueryTracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("PrepareEnd")

	attrs := []attribute.KeyValue{}
	// done
	t.stop(span, data.Err, attrs)
}

// TraceQueryStart implements pgx.QueryTracer.
func (t *QueryTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	attrs := []attribute.KeyValue{}
	attrs = append(attrs, t.config(conn.Config())...)
	attrs = append(attrs, t.statement(data.SQL))
	// prepare the context
	ctx, span := t.start(ctx, data.SQL, attrs)
	span.AddEvent("QueryStart")
	// done!
	return ctx
}

// TraceQueryEnd implements pgx.QueryTracer.
func (t *QueryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("QueryEnd")

	attrs := []attribute.KeyValue{}
	// done
	t.stop(span, data.Err, attrs)
}

// TraceCopyFromStart implements pgx.CopyFromTracer.
func (t *QueryTracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	// attributes
	attrs := []attribute.KeyValue{}
	attrs = append(attrs, t.config(conn.Config())...)
	attrs = append(attrs, t.collection(data.TableName))
	// prepare the context
	ctx, span := t.start(ctx, "Copy", attrs)
	span.AddEvent("CopyFromStart")
	// done!
	return ctx
}

// TraceCopyFromEnd implements pgx.CopyFromTracer.
func (t *QueryTracer) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("CopyFromEnd")

	attrs := []attribute.KeyValue{}
	attrs = append(attrs, t.command(data.CommandTag))
	// done!
	t.stop(span, data.Err, attrs)
}

// TraceBatchStart implements pgx.BatchTracer.
func (t *QueryTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	attrs := []attribute.KeyValue{}
	attrs = append(attrs, t.config(conn.Config())...)
	// prepare the context
	ctx, _ = t.start(ctx, "BatchStart", attrs)
	// done!
	return ctx
}

// TraceBatchQuery implements pgx.BatchTracer.
func (t *QueryTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	attrs := []attribute.KeyValue{}
	attrs = append(attrs, t.config(conn.Config())...)
	attrs = append(attrs, t.command(data.CommandTag))
	attrs = append(attrs, t.statement(data.SQL))

	// prepare the context
	_, span := t.start(ctx, data.SQL, attrs)
	span.AddEvent("BatchQuery")
	// done!
	t.stop(span, data.Err, attrs)
}

// TraceBatchEnd implements pgx.BatchTracer.
func (t *QueryTracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("BatchEnd")

	attrs := []attribute.KeyValue{}
	// done
	t.stop(span, data.Err, attrs)
}

func (q *QueryTracer) tracer() trace.Tracer {
	// get the tracer
	return otel.GetTracerProvider().Tracer(q.Name, q.Options...)
}

var pattern = regexp.MustCompile(`^--\s+name:\s+(\w+)`)

func (q *QueryTracer) start(ctx context.Context, name string, attrs []attribute.KeyValue) (context.Context, trace.Span) {
	if match := pattern.FindStringSubmatch(name); len(match) == 2 {
		name = match[1]
	}

	options := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	}

	return q.tracer().Start(ctx, name, options...)
}

func (t *QueryTracer) stop(span trace.Span, err error, attrs []attribute.KeyValue) {
	defer span.End()
	// set the attributes
	for _, attr := range attrs {
		if attr.Valid() {
			span.SetAttributes(attr)
		}
	}

	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			if !errors.Is(err, pgx.ErrNoRows) {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
		}
	}
}

func (t *QueryTracer) config(config *pgx.ConnConfig) []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.DBSystemPostgreSQL,
		semconv.DBUser(config.User),
		semconv.DBName(config.Database),
		semconv.DBConnectionString(t.connection(config)),
	}
}

func (t *QueryTracer) connection(config *pgx.ConnConfig) string {
	conn := config.ConnString()
	conn = strings.ReplaceAll(conn, config.Password, strings.Repeat("*", len(config.Password)))
	return conn
}

func (q *QueryTracer) command(command pgconn.CommandTag) attribute.KeyValue {
	name := "UNKNOWN"

	switch {
	case command.Select():
		name = "SELECT"
	case command.Insert():
		name = "INSERT"
	case command.Delete():
		name = "DELETE"
	case command.Update():
		name = "UPDATE"
	}

	return semconv.DBOperation(name)
}

func (t *QueryTracer) collection(name pgx.Identifier) attribute.KeyValue {
	return semconv.DBSQLTable(name.Sanitize())
}

func (q *QueryTracer) statement(query string) attribute.KeyValue {
	reader := strings.NewReader(query)
	scanner := bufio.NewScanner(reader)

	builder := &strings.Builder{}
	// scan the query and fill the builder
	for scanner.Scan() {
		text := scanner.Text()
		text = strings.TrimSpace(text)

		index := strings.Index(text, "--")

		if index == 0 {
			continue
		}

		if index > 0 {
			text = text[:index]
		}

		text = strings.TrimSpace(text)

		if builder.Len() > 0 {
			builder.WriteString(" ")
		}

		builder.WriteString(text)
	}

	statement := builder.String()
	// done
	return semconv.DBStatement(statement)
}
