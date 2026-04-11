package pgxotel

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"

	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	pgxpool "github.com/jackc/pgx/v5/pgxpool"

	otel "go.opentelemetry.io/otel"
	attribute "go.opentelemetry.io/otel/attribute"
	codes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	trace "go.opentelemetry.io/otel/trace"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// findSpanByName returns the first span with the given name, or nil.
func findSpanByName(spans tracetest.SpanStubs, name string) *tracetest.SpanStub {
	for i := range spans {
		if spans[i].Name == name {
			return &spans[i]
		}
	}
	return nil
}

// spanEventNames returns the names of all events on a span.
func spanEventNames(s *tracetest.SpanStub) []string {
	names := make([]string, len(s.Events))
	for i, e := range s.Events {
		names[i] = e.Name
	}
	return names
}

// findAttr returns a pointer to the first attribute with the given key string, or nil.
func findAttr(attrs []attribute.KeyValue, key string) *attribute.KeyValue {
	for i := range attrs {
		if string(attrs[i].Key) == key {
			return &attrs[i]
		}
	}
	return nil
}

var _ = Describe("pgxotel unit tests", func() {
	var (
		exporter *tracetest.InMemoryExporter
		tp       *sdktrace.TracerProvider
		qt       *QueryTracer
	)

	BeforeEach(func() {
		exporter = tracetest.NewInMemoryExporter()
		tp = sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
		otel.SetTracerProvider(tp)
		qt = &QueryTracer{Name: "test-tracer"}
	})

	AfterEach(func() {
		Expect(tp.Shutdown(context.Background())).To(Succeed())
	})

	// -------------------------------------------------------------------------
	Describe("tracer()", func() {
		It("returns a non-nil tracer using the Name field", func() {
			tr := qt.tracer()
			Expect(tr).NotTo(BeNil())
		})

		It("picks up a changed global TracerProvider", func() {
			exporter2 := tracetest.NewInMemoryExporter()
			tp2 := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter2))
			otel.SetTracerProvider(tp2)

			qt2 := &QueryTracer{Name: "other-tracer"}
			tr := qt2.tracer()
			Expect(tr).NotTo(BeNil())

			Expect(tp2.Shutdown(context.Background())).To(Succeed())
		})
	})

	// -------------------------------------------------------------------------
	Describe("start()", func() {
		It("uses raw SQL string as span name when no -- name: comment", func() {
			ctx := context.Background()
			ctx, parentSpan := tp.Tracer("test").Start(ctx, "parent")
			defer parentSpan.End()

			_, span := qt.start(ctx, "SELECT 1", nil)
			span.End()

			spans := exporter.GetSpans()
			s := findSpanByName(spans, "SELECT 1")
			Expect(s).NotTo(BeNil())
		})

		It("extracts span name from -- name: Foo prefix via regex", func() {
			cases := []struct {
				sql      string
				wantName string
			}{
				{"-- name: FindUser\nSELECT * FROM users WHERE id = $1", "FindUser"},
				{"-- name: CreateOrder\nINSERT INTO orders VALUES ($1)", "CreateOrder"},
				{"-- name: ListItems\nSELECT id FROM items", "ListItems"},
			}

			ctx := context.Background()
			ctx, parentSpan := tp.Tracer("test").Start(ctx, "parent")
			defer parentSpan.End()

			for _, tc := range cases {
				exporter.Reset()
				_, span := qt.start(ctx, tc.sql, nil)
				span.End()

				spans := exporter.GetSpans()
				s := findSpanByName(spans, tc.wantName)
				Expect(s).NotTo(BeNil(), "expected span named %q for sql %q", tc.wantName, tc.sql)
			}
		})

		It("sets SpanKindClient on every created span", func() {
			ctx := context.Background()
			ctx, parentSpan := tp.Tracer("test").Start(ctx, "parent")
			defer parentSpan.End()

			_, span := qt.start(ctx, "SELECT 1", nil)
			span.End()

			spans := exporter.GetSpans()
			s := findSpanByName(spans, "SELECT 1")
			Expect(s).NotTo(BeNil())
			Expect(s.SpanKind).To(Equal(trace.SpanKindClient))
		})

		It("attaches provided attributes to the span", func() {
			ctx := context.Background()
			ctx, parentSpan := tp.Tracer("test").Start(ctx, "parent")
			defer parentSpan.End()

			attrs := []attribute.KeyValue{
				attribute.String("db.system", "postgresql"),
				attribute.String("db.name", "testdb"),
			}
			_, span := qt.start(ctx, "SELECT 1", attrs)
			span.End()

			spans := exporter.GetSpans()
			s := findSpanByName(spans, "SELECT 1")
			Expect(s).NotTo(BeNil())

			dbSystem := findAttr(s.Attributes, "db.system")
			Expect(dbSystem).NotTo(BeNil())
			Expect(dbSystem.Value.AsString()).To(Equal("postgresql"))
		})
	})

	// -------------------------------------------------------------------------
	Describe("stop()", func() {
		makeSpan := func(ctx context.Context) trace.Span {
			_, span := tp.Tracer("test").Start(ctx, "op")
			return span
		}

		It("ends span with codes.Unset when err == nil", func() {
			ctx := context.Background()
			span := makeSpan(ctx)
			qt.stop(span, nil, nil)

			spans := exporter.GetSpans()
			Expect(spans).To(HaveLen(1))
			Expect(spans[0].Status.Code).To(Equal(codes.Unset))
		})

		It("does NOT record error for sql.ErrNoRows", func() {
			ctx := context.Background()
			span := makeSpan(ctx)
			qt.stop(span, sql.ErrNoRows, nil)

			spans := exporter.GetSpans()
			Expect(spans).To(HaveLen(1))
			Expect(spans[0].Status.Code).To(Equal(codes.Unset))

			for _, e := range spans[0].Events {
				Expect(e.Name).NotTo(Equal("exception"))
			}
		})

		It("does NOT record error for pgx.ErrNoRows", func() {
			ctx := context.Background()
			span := makeSpan(ctx)
			qt.stop(span, pgx.ErrNoRows, nil)

			spans := exporter.GetSpans()
			Expect(spans).To(HaveLen(1))
			Expect(spans[0].Status.Code).To(Equal(codes.Unset))

			for _, e := range spans[0].Events {
				Expect(e.Name).NotTo(Equal("exception"))
			}
		})

		It("records error and sets codes.Error for any other error", func() {
			ctx := context.Background()
			span := makeSpan(ctx)
			err := errors.New("connection refused")
			qt.stop(span, err, nil)

			spans := exporter.GetSpans()
			Expect(spans).To(HaveLen(1))
			Expect(spans[0].Status.Code).To(Equal(codes.Error))
			Expect(spans[0].Status.Description).To(Equal("connection refused"))

			eventNames := spanEventNames(&spans[0])
			Expect(eventNames).To(ContainElement("exception"))
		})

		It("sets valid attributes on the span when provided", func() {
			ctx := context.Background()
			span := makeSpan(ctx)
			attrs := []attribute.KeyValue{
				attribute.String("db.operation", "SELECT"),
			}
			qt.stop(span, nil, attrs)

			spans := exporter.GetSpans()
			Expect(spans).To(HaveLen(1))
			dbOp := findAttr(spans[0].Attributes, "db.operation")
			Expect(dbOp).NotTo(BeNil())
			Expect(dbOp.Value.AsString()).To(Equal("SELECT"))
		})
	})

	// -------------------------------------------------------------------------
	Describe("config()", func() {
		var connConfig *pgx.ConnConfig

		BeforeEach(func() {
			var err error
			connConfig, err = pgx.ParseConfig("host=localhost user=testuser password=secret dbname=testdb")
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns semconv.DBSystemPostgreSQL", func() {
			attrs := qt.config(connConfig)
			dbSystem := findAttr(attrs, "db.system")
			Expect(dbSystem).NotTo(BeNil())
			Expect(dbSystem.Value.AsString()).To(Equal(semconv.DBSystemPostgreSQL.Value.AsString()))
		})

		It("returns db.user from ConnConfig.User", func() {
			attrs := qt.config(connConfig)
			dbUser := findAttr(attrs, "db.user")
			Expect(dbUser).NotTo(BeNil())
			Expect(dbUser.Value.AsString()).To(Equal("testuser"))
		})

		It("returns db.name from ConnConfig.Database", func() {
			attrs := qt.config(connConfig)
			dbName := findAttr(attrs, "db.name")
			Expect(dbName).NotTo(BeNil())
			Expect(dbName.Value.AsString()).To(Equal("testdb"))
		})

		It("returns db.connection_string with password masked", func() {
			attrs := qt.config(connConfig)
			dbConn := findAttr(attrs, "db.connection_string")
			Expect(dbConn).NotTo(BeNil())
			Expect(dbConn.Value.AsString()).NotTo(ContainSubstring("secret"))
			Expect(dbConn.Value.AsString()).To(ContainSubstring("******"))
		})
	})

	// -------------------------------------------------------------------------
	Describe("connection()", func() {
		DescribeTable("password masking",
			func(password, notExpected, expectedMask string) {
				connConfig, err := pgx.ParseConfig(
					"host=localhost user=u dbname=db password=" + password,
				)
				Expect(err).NotTo(HaveOccurred())

				result := qt.connection(connConfig)
				if notExpected != "" {
					Expect(result).NotTo(ContainSubstring(notExpected))
				}
				if expectedMask != "" {
					Expect(result).To(ContainSubstring(expectedMask))
				}
			},
			Entry("replaces password with asterisks", "secret", "secret", strings.Repeat("*", len("secret"))),
			Entry("replaces longer password", "myLongP@ssw0rd", "myLongP@ssw0rd", strings.Repeat("*", len("myLongP@ssw0rd"))),
		)

		It("returns non-empty string when no password", func() {
			connConfig, err := pgx.ParseConfig("host=localhost user=u dbname=db")
			Expect(err).NotTo(HaveOccurred())
			result := qt.connection(connConfig)
			Expect(result).NotTo(BeEmpty())
		})
	})

	// -------------------------------------------------------------------------
	Describe("command()", func() {
		DescribeTable("maps CommandTag to db.operation",
			func(tagStr string, expectedOp string) {
				tag := pgconn.NewCommandTag(tagStr)
				kv := qt.command(tag)
				Expect(string(kv.Key)).To(Equal("db.operation"))
				Expect(kv.Value.AsString()).To(Equal(expectedOp))
			},
			Entry("SELECT", "SELECT 5", "SELECT"),
			Entry("INSERT", "INSERT 0 1", "INSERT"),
			Entry("UPDATE", "UPDATE 3", "UPDATE"),
			Entry("DELETE", "DELETE 2", "DELETE"),
			Entry("COPY", "COPY 10", "UNKNOWN"),
			Entry("empty", "", "UNKNOWN"),
		)
	})

	// -------------------------------------------------------------------------
	Describe("collection()", func() {
		DescribeTable("sanitizes table identifier",
			func(parts []string, expected string) {
				id := pgx.Identifier(parts)
				kv := qt.collection(id)
				Expect(string(kv.Key)).To(Equal("db.sql.table"))
				Expect(kv.Value.AsString()).To(Equal(expected))
			},
			Entry("single part", []string{"users"}, `"users"`),
			Entry("schema qualified", []string{"public", "orders"}, `"public"."orders"`),
		)
	})

	// -------------------------------------------------------------------------
	Describe("statement()", func() {
		DescribeTable("processes SQL",
			func(sql string, expected string) {
				kv := qt.statement(sql)
				Expect(string(kv.Key)).To(Equal("db.statement"))
				Expect(kv.Value.AsString()).To(Equal(expected))
			},
			Entry("plain single-line SQL",
				"SELECT id FROM users",
				"SELECT id FROM users",
			),
			Entry("full-line comment stripped",
				"-- this is a comment\nSELECT 1",
				"SELECT 1",
			),
			Entry("inline comment stripped",
				"SELECT 1 -- inline comment",
				"SELECT 1",
			),
			Entry("multi-line SQL joined to single line",
				"SELECT id\nFROM users\nWHERE id = $1",
				"SELECT id FROM users WHERE id = $1",
			),
			Entry("-- name: comment + SQL — only SQL remains",
				"-- name: FindUser\nSELECT * FROM users WHERE id = $1",
				"SELECT * FROM users WHERE id = $1",
			),
			Entry("all-comment query returns empty string",
				"-- just a comment\n-- another comment",
				"",
			),
		)
	})

	// -------------------------------------------------------------------------
	Describe("TraceConnectStart / TraceConnectEnd", func() {
		var connConfig *pgx.ConnConfig

		BeforeEach(func() {
			var err error
			connConfig, err = pgx.ParseConfig("host=localhost user=test password=secret dbname=db")
			Expect(err).NotTo(HaveOccurred())
		})

		It("non-recording parent context returns same ctx, no spans exported", func() {
			ctx := context.Background()
			// no parent span → SpanFromContext returns noopSpan (not recording)
			resultCtx := qt.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: connConfig})
			Expect(resultCtx).To(Equal(ctx))
			Expect(exporter.GetSpans()).To(BeEmpty())
		})

		It("recording parent context creates child span named Connect with SpanKindClient", func() {
			ctx := context.Background()
			ctx, parentSpan := tp.Tracer("test").Start(ctx, "parent")
			defer parentSpan.End()

			ctx = qt.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: connConfig})
			qt.TraceConnectEnd(ctx, pgx.TraceConnectEndData{})

			parentSpan.End()

			spans := exporter.GetSpans()
			s := findSpanByName(spans, "Connect")
			Expect(s).NotTo(BeNil())
			Expect(s.SpanKind).To(Equal(trace.SpanKindClient))
		})

		It("TraceConnectStart adds ConnectStart event; TraceConnectEnd adds ConnectEnd event", func() {
			ctx := context.Background()
			ctx, parentSpan := tp.Tracer("test").Start(ctx, "parent")
			defer parentSpan.End()

			ctx = qt.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: connConfig})
			qt.TraceConnectEnd(ctx, pgx.TraceConnectEndData{})

			parentSpan.End()

			spans := exporter.GetSpans()
			s := findSpanByName(spans, "Connect")
			Expect(s).NotTo(BeNil())
			Expect(spanEventNames(s)).To(ContainElements("ConnectStart", "ConnectEnd"))
		})

		It("db.connection_string attribute has password masked", func() {
			ctx := context.Background()
			ctx, parentSpan := tp.Tracer("test").Start(ctx, "parent")
			defer parentSpan.End()

			ctx = qt.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: connConfig})
			qt.TraceConnectEnd(ctx, pgx.TraceConnectEndData{})

			parentSpan.End()

			spans := exporter.GetSpans()
			s := findSpanByName(spans, "Connect")
			Expect(s).NotTo(BeNil())
			connStr := findAttr(s.Attributes, "db.connection_string")
			Expect(connStr).NotTo(BeNil())
			Expect(connStr.Value.AsString()).NotTo(ContainSubstring("secret"))
		})

		It("TraceConnectEnd with nil error → codes.Unset", func() {
			ctx := context.Background()
			ctx, parentSpan := tp.Tracer("test").Start(ctx, "parent")
			defer parentSpan.End()

			ctx = qt.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: connConfig})
			qt.TraceConnectEnd(ctx, pgx.TraceConnectEndData{Err: nil})

			parentSpan.End()

			spans := exporter.GetSpans()
			s := findSpanByName(spans, "Connect")
			Expect(s).NotTo(BeNil())
			Expect(s.Status.Code).To(Equal(codes.Unset))
		})

		It("TraceConnectEnd with error → codes.Error + description", func() {
			ctx := context.Background()
			ctx, parentSpan := tp.Tracer("test").Start(ctx, "parent")
			defer parentSpan.End()

			connErr := errors.New("dial tcp: connection refused")
			ctx = qt.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: connConfig})
			qt.TraceConnectEnd(ctx, pgx.TraceConnectEndData{Err: connErr})

			parentSpan.End()

			spans := exporter.GetSpans()
			s := findSpanByName(spans, "Connect")
			Expect(s).NotTo(BeNil())
			Expect(s.Status.Code).To(Equal(codes.Error))
			Expect(s.Status.Description).To(Equal("dial tcp: connection refused"))
		})
	})

	// -------------------------------------------------------------------------
	Describe("Integration", Ordered, func() {
		var (
			pool    *pgxpool.Pool
			intCtx  context.Context
			intSpan trace.Span
		)

		BeforeAll(func() {
			dbURL := os.Getenv("PGX_DATABASE_URL")
			if dbURL == "" {
				Skip("PGX_DATABASE_URL not set")
			}

			config, err := pgxpool.ParseConfig(dbURL)
			Expect(err).NotTo(HaveOccurred())

			config.ConnConfig.Tracer = qt

			pool, err = pgxpool.NewWithConfig(context.Background(), config)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterAll(func() {
			if pool != nil {
				pool.Close()
			}
		})

		BeforeEach(func() {
			intCtx, intSpan = tp.Tracer("test").Start(context.Background(), "parent")
			exporter.Reset()
		})

		AfterEach(func() {
			intSpan.End()
		})

		Describe("TraceQueryStart/End", func() {
			It("creates span named after SQL with QueryStart + QueryEnd events", func() {
				sql := "SELECT 1"
				rows, err := pool.Query(intCtx, sql)
				Expect(err).NotTo(HaveOccurred())
				rows.Close()

				intSpan.End()

				spans := exporter.GetSpans()
				s := findSpanByName(spans, sql)
				Expect(s).NotTo(BeNil())
				Expect(spanEventNames(s)).To(ContainElements("QueryStart", "QueryEnd"))
			})

			It("names span from -- name: comment", func() {
				sql := "-- name: HelloWorld\nSELECT 1"
				rows, err := pool.Query(intCtx, sql)
				Expect(err).NotTo(HaveOccurred())
				rows.Close()

				intSpan.End()

				spans := exporter.GetSpans()
				s := findSpanByName(spans, "HelloWorld")
				Expect(s).NotTo(BeNil())
			})

			It("db.statement attribute has comments stripped", func() {
				sql := "-- name: Stripped\nSELECT 42"
				rows, err := pool.Query(intCtx, sql)
				Expect(err).NotTo(HaveOccurred())
				rows.Close()

				intSpan.End()

				spans := exporter.GetSpans()
				s := findSpanByName(spans, "Stripped")
				Expect(s).NotTo(BeNil())
				stmt := findAttr(s.Attributes, "db.statement")
				Expect(stmt).NotTo(BeNil())
				Expect(stmt.Value.AsString()).NotTo(ContainSubstring("--"))
				Expect(stmt.Value.AsString()).To(ContainSubstring("SELECT 42"))
			})

			It("pgx.ErrNoRows does NOT set codes.Error on span", func() {
				// QueryRow + Scan on a query returning no rows triggers pgx.ErrNoRows
				var val int
				err := pool.QueryRow(intCtx, "SELECT 1 WHERE 1=0").Scan(&val)
				Expect(errors.Is(err, pgx.ErrNoRows)).To(BeTrue())

				intSpan.End()

				spans := exporter.GetSpans()
				// Find any query span
				var querySpan *tracetest.SpanStub
				for i := range spans {
					if findAttr(spans[i].Attributes, "db.statement") != nil {
						querySpan = &spans[i]
						break
					}
				}
				Expect(querySpan).NotTo(BeNil())
				Expect(querySpan.Status.Code).NotTo(Equal(codes.Error))
			})
		})

		Describe("TracePrepareStart/End", func() {
			It("creates span with PrepareStart + PrepareEnd events", func() {
				conn, err := pool.Acquire(intCtx)
				Expect(err).NotTo(HaveOccurred())
				defer conn.Release()

				_, err = conn.Conn().Prepare(intCtx, "stmt1", "SELECT $1::int")
				Expect(err).NotTo(HaveOccurred())

				intSpan.End()

				spans := exporter.GetSpans()
				var prepSpan *tracetest.SpanStub
				for i := range spans {
					if strings.Contains(spanEventNames(&spans[i])[0], "Prepare") {
						prepSpan = &spans[i]
						break
					}
				}
				// Find any span with PrepareStart event
				for i := range spans {
					evts := spanEventNames(&spans[i])
					hasPrep := false
					for _, e := range evts {
						if e == "PrepareStart" {
							hasPrep = true
						}
					}
					if hasPrep {
						prepSpan = &spans[i]
						break
					}
				}
				Expect(prepSpan).NotTo(BeNil())
				Expect(spanEventNames(prepSpan)).To(ContainElements("PrepareStart", "PrepareEnd"))
			})
		})

		Describe("TraceCopyFromStart/End", func() {
			BeforeEach(func() {
				conn, err := pool.Acquire(intCtx)
				Expect(err).NotTo(HaveOccurred())
				defer conn.Release()
				_, _ = conn.Exec(intCtx, "CREATE TABLE IF NOT EXISTS pgxotel_copy_test (id int)")
				_, _ = conn.Exec(intCtx, "TRUNCATE pgxotel_copy_test")
				exporter.Reset()
			})

			AfterEach(func() {
				conn, err := pool.Acquire(intCtx)
				Expect(err).NotTo(HaveOccurred())
				defer conn.Release()
				_, _ = conn.Exec(intCtx, "DROP TABLE IF EXISTS pgxotel_copy_test")
			})

			It("creates span named Copy with CopyFromStart + CopyFromEnd events", func() {
				rows := [][]interface{}{{1}, {2}, {3}}
				_, err := pool.CopyFrom(
					intCtx,
					pgx.Identifier{"pgxotel_copy_test"},
					[]string{"id"},
					pgx.CopyFromRows(rows),
				)
				Expect(err).NotTo(HaveOccurred())

				intSpan.End()

				spans := exporter.GetSpans()
				s := findSpanByName(spans, "Copy")
				Expect(s).NotTo(BeNil())
				Expect(spanEventNames(s)).To(ContainElements("CopyFromStart", "CopyFromEnd"))
			})

			It("db.sql.table attribute contains sanitized table name", func() {
				rows := [][]interface{}{{4}}
				_, err := pool.CopyFrom(
					intCtx,
					pgx.Identifier{"pgxotel_copy_test"},
					[]string{"id"},
					pgx.CopyFromRows(rows),
				)
				Expect(err).NotTo(HaveOccurred())

				intSpan.End()

				spans := exporter.GetSpans()
				s := findSpanByName(spans, "Copy")
				Expect(s).NotTo(BeNil())
				tbl := findAttr(s.Attributes, "db.sql.table")
				Expect(tbl).NotTo(BeNil())
				Expect(tbl.Value.AsString()).To(ContainSubstring("pgxotel_copy_test"))
			})
		})

		Describe("TraceBatchStart/BatchQuery/BatchEnd", func() {
			It("creates BatchStart span with BatchEnd event and per-SQL child spans with BatchQuery event", func() {
				batch := &pgx.Batch{}
				batch.Queue("SELECT 1")
				batch.Queue("SELECT 2")

				results := pool.SendBatch(intCtx, batch)
				_, err := results.Exec()
				Expect(err).NotTo(HaveOccurred())
				_, err = results.Exec()
				Expect(err).NotTo(HaveOccurred())
				Expect(results.Close()).To(Succeed())

				intSpan.End()

				spans := exporter.GetSpans()

				batchSpan := findSpanByName(spans, "BatchStart")
				Expect(batchSpan).NotTo(BeNil())
				Expect(spanEventNames(batchSpan)).To(ContainElement("BatchEnd"))

				// Each queued SQL should have its own span with BatchQuery event
				var batchQuerySpans []*tracetest.SpanStub
				for i := range spans {
					evts := spanEventNames(&spans[i])
					for _, e := range evts {
						if e == "BatchQuery" {
							batchQuerySpans = append(batchQuerySpans, &spans[i])
							break
						}
					}
				}
				Expect(batchQuerySpans).To(HaveLen(2))
			})
		})
	})
})
