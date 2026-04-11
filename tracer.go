package pgxotel

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"sync"

	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	otel "go.opentelemetry.io/otel"
	attribute "go.opentelemetry.io/otel/attribute"
	codes "go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	trace "go.opentelemetry.io/otel/trace"
)

var (
	_ pgx.QueryTracer    = (*QueryTracer)(nil)
	_ pgx.BatchTracer    = (*QueryTracer)(nil)
	_ pgx.ConnectTracer  = (*QueryTracer)(nil)
	_ pgx.PrepareTracer  = (*QueryTracer)(nil)
	_ pgx.CopyFromTracer = (*QueryTracer)(nil)
)

// QueryTracer instruments pgx with OpenTelemetry tracing.
type QueryTracer struct {
	// Name is the instrumentation scope name used when creating spans.
	Name string
	// Options are additional options for the tracer.
	Options []trace.TracerOption
	// Provider is the TracerProvider to use. Falls back to the global
	// provider when nil. The provider is resolved once on first use and
	// cached for the lifetime of the QueryTracer.
	Provider trace.TracerProvider
	// IncludeStatement controls whether the sanitized SQL is recorded as
	// the db.query.text span attribute. Off by default — enable only when
	// the SQL content is not considered sensitive in your environment.
	IncludeStatement bool

	once         sync.Once
	cachedTracer trace.Tracer
}

// TraceConnectStart implements pgx.ConnectTracer.
func (t *QueryTracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}
	ctx, _ = t.start(ctx, "Connect", t.config(data.ConnConfig))
	return ctx
}

// TraceConnectEnd implements pgx.ConnectTracer.
func (t *QueryTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	t.stop(trace.SpanFromContext(ctx), data.Err)
}

// TracePrepareStart implements pgx.PrepareTracer.
func (t *QueryTracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}
	attrs := t.config(conn.Config())
	if kv := t.queryText(data.SQL); kv != nil {
		attrs = append(attrs, *kv)
	}
	ctx, _ = t.start(ctx, spanName(data.SQL), attrs)
	return ctx
}

// TracePrepareEnd implements pgx.PrepareTracer.
func (t *QueryTracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	t.stop(trace.SpanFromContext(ctx), data.Err)
}

// TraceQueryStart implements pgx.QueryTracer.
func (t *QueryTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}
	attrs := t.config(conn.Config())
	if kv := t.queryText(data.SQL); kv != nil {
		attrs = append(attrs, *kv)
	}
	ctx, _ = t.start(ctx, spanName(data.SQL), attrs)
	return ctx
}

// TraceQueryEnd implements pgx.QueryTracer.
func (t *QueryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(t.operationName(data.CommandTag))
	t.stop(span, data.Err)
}

// TraceCopyFromStart implements pgx.CopyFromTracer.
func (t *QueryTracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}
	attrs := t.config(conn.Config())
	attrs = append(attrs, t.collection(data.TableName))
	ctx, _ = t.start(ctx, "Copy", attrs)
	return ctx
}

// TraceCopyFromEnd implements pgx.CopyFromTracer.
func (t *QueryTracer) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(t.operationName(data.CommandTag))
	t.stop(span, data.Err)
}

// TraceBatchStart implements pgx.BatchTracer.
func (t *QueryTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}
	ctx, _ = t.start(ctx, "BatchStart", t.config(conn.Config()))
	return ctx
}

// TraceBatchQuery implements pgx.BatchTracer.
func (t *QueryTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	attrs := t.config(conn.Config())
	attrs = append(attrs, t.operationName(data.CommandTag))
	if kv := t.queryText(data.SQL); kv != nil {
		attrs = append(attrs, *kv)
	}
	_, span := t.start(ctx, spanName(data.SQL), attrs)
	t.stop(span, data.Err)
}

// TraceBatchEnd implements pgx.BatchTracer.
func (t *QueryTracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	t.stop(trace.SpanFromContext(ctx), data.Err)
}

// tracer returns the cached Tracer, initialising it on first call.
func (q *QueryTracer) tracer() trace.Tracer {
	q.once.Do(func() {
		p := q.Provider
		if p == nil {
			p = otel.GetTracerProvider()
		}
		q.cachedTracer = p.Tracer(q.Name, q.Options...)
	})
	return q.cachedTracer
}

var (
	namePattern    = regexp.MustCompile(`^--\s+name:\s+(\w+)`)
	keywordPattern = regexp.MustCompile(`(?i)^\s*(SELECT|INSERT|UPDATE|DELETE|COPY|CALL|EXECUTE|BEGIN|COMMIT|ROLLBACK|CREATE|DROP|ALTER|TRUNCATE|EXPLAIN)\b`)
)

// spanName returns a low-cardinality span name for a SQL string.
//
//   - "-- name: Foo" prefix → "Foo"
//   - first SQL keyword (SELECT, INSERT, …) → upper-cased keyword
//   - anything else → "db.query"
func spanName(sql string) string {
	if match := namePattern.FindStringSubmatch(sql); len(match) == 2 {
		return match[1]
	}
	if match := keywordPattern.FindStringSubmatch(sanitizeSQL(sql)); len(match) == 2 {
		return strings.ToUpper(match[1])
	}
	return "db.query"
}

func (q *QueryTracer) start(ctx context.Context, name string, attrs []attribute.KeyValue) (context.Context, trace.Span) {
	return q.tracer().Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
}

func (t *QueryTracer) stop(span trace.Span, err error) {
	defer span.End()
	if err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, pgx.ErrNoRows) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

func (t *QueryTracer) config(config *pgx.ConnConfig) []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.DBSystemNamePostgreSQL,
		semconv.DBNamespace(config.Database),
		semconv.ServerAddress(config.Host),
		semconv.ServerPort(int(config.Port)),
	}
}

func (q *QueryTracer) operationName(command pgconn.CommandTag) attribute.KeyValue {
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
	return semconv.DBOperationName(name)
}

func (t *QueryTracer) collection(name pgx.Identifier) attribute.KeyValue {
	return semconv.DBCollectionName(name.Sanitize())
}

// queryText returns a db.query.text attribute with the sanitized SQL when
// IncludeStatement is true, and nil otherwise.
func (t *QueryTracer) queryText(query string) *attribute.KeyValue {
	if !t.IncludeStatement {
		return nil
	}
	kv := semconv.DBQueryText(sanitizeSQL(query))
	return &kv
}

// sanitizeSQL strips SQL comments and collapses the query to a single line.
func sanitizeSQL(query string) string {
	scanner := bufio.NewScanner(strings.NewReader(query))
	var b strings.Builder
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if idx := strings.Index(text, "--"); idx == 0 {
			continue
		} else if idx > 0 {
			text = strings.TrimSpace(text[:idx])
		}
		if text == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(text)
	}
	return b.String()
}
