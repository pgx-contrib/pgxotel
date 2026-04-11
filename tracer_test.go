package pgxotel

import (
	"context"
	"database/sql"
	"errors"
	"os"

	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	pgxpool "github.com/jackc/pgx/v5/pgxpool"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	otel "go.opentelemetry.io/otel"
	attribute "go.opentelemetry.io/otel/attribute"
	codes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
	trace "go.opentelemetry.io/otel/trace"
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

// findAttr returns a pointer to the first attribute with the given key, or nil.
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
			Expect(qt.tracer()).NotTo(BeNil())
		})

		It("uses the Provider field when set", func() {
			exporter2 := tracetest.NewInMemoryExporter()
			tp2 := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter2))
			defer tp2.Shutdown(context.Background()) //nolint:errcheck

			qt2 := &QueryTracer{Name: "scoped", Provider: tp2}
			ctx, parentSpan := tp2.Tracer("test").Start(context.Background(), "parent")
			defer parentSpan.End()

			_, span := qt2.start(ctx, "SELECT", nil)
			span.End()

			// span should appear in tp2's exporter, not the global one
			Expect(exporter2.GetSpans()).NotTo(BeEmpty())
			Expect(exporter.GetSpans()).To(BeEmpty())
		})

		It("caches the tracer after first call", func() {
			t1 := qt.tracer()
			t2 := qt.tracer()
			Expect(t1).To(BeIdenticalTo(t2))
		})
	})

	// -------------------------------------------------------------------------
	Describe("spanName()", func() {
		DescribeTable("derives low-cardinality names",
			func(sql, expected string) {
				Expect(spanName(sql)).To(Equal(expected))
			},
			Entry("-- name: comment", "-- name: FindUser\nSELECT 1", "FindUser"),
			Entry("SELECT keyword", "SELECT id FROM users", "SELECT"),
			Entry("INSERT keyword", "INSERT INTO users VALUES ($1)", "INSERT"),
			Entry("UPDATE keyword", "UPDATE users SET name=$1", "UPDATE"),
			Entry("DELETE keyword", "DELETE FROM users WHERE id=$1", "DELETE"),
			Entry("lowercase select", "select * from t", "SELECT"),
			Entry("mixed-case begin", "Begin", "BEGIN"),
			Entry("unrecognised SQL falls back", "VACUUM ANALYZE users", "db.query"),
			Entry("empty string falls back", "", "db.query"),
		)
	})

	// -------------------------------------------------------------------------
	Describe("start()", func() {
		It("creates a span with the exact name provided", func() {
			ctx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
			defer parentSpan.End()

			_, span := qt.start(ctx, "Connect", nil)
			span.End()

			Expect(findSpanByName(exporter.GetSpans(), "Connect")).NotTo(BeNil())
		})

		It("sets SpanKindClient on every created span", func() {
			ctx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
			defer parentSpan.End()

			_, span := qt.start(ctx, "SELECT", nil)
			span.End()

			s := findSpanByName(exporter.GetSpans(), "SELECT")
			Expect(s).NotTo(BeNil())
			Expect(s.SpanKind).To(Equal(trace.SpanKindClient))
		})

		It("attaches provided attributes to the span", func() {
			ctx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
			defer parentSpan.End()

			attrs := []attribute.KeyValue{attribute.String("db.namespace", "testdb")}
			_, span := qt.start(ctx, "SELECT", attrs)
			span.End()

			s := findSpanByName(exporter.GetSpans(), "SELECT")
			Expect(s).NotTo(BeNil())
			Expect(findAttr(s.Attributes, "db.namespace")).NotTo(BeNil())
		})
	})

	// -------------------------------------------------------------------------
	Describe("stop()", func() {
		makeSpan := func() trace.Span {
			_, span := tp.Tracer("test").Start(context.Background(), "op")
			return span
		}

		It("ends span with codes.Unset when err is nil", func() {
			qt.stop(makeSpan(), nil)
			Expect(exporter.GetSpans()[0].Status.Code).To(Equal(codes.Unset))
		})

		It("does NOT record error for sql.ErrNoRows", func() {
			qt.stop(makeSpan(), sql.ErrNoRows)
			s := exporter.GetSpans()[0]
			Expect(s.Status.Code).To(Equal(codes.Unset))
			for _, e := range s.Events {
				Expect(e.Name).NotTo(Equal("exception"))
			}
		})

		It("does NOT record error for pgx.ErrNoRows", func() {
			qt.stop(makeSpan(), pgx.ErrNoRows)
			s := exporter.GetSpans()[0]
			Expect(s.Status.Code).To(Equal(codes.Unset))
			for _, e := range s.Events {
				Expect(e.Name).NotTo(Equal("exception"))
			}
		})

		It("records error and sets codes.Error for any other error", func() {
			qt.stop(makeSpan(), errors.New("connection refused"))
			s := exporter.GetSpans()[0]
			Expect(s.Status.Code).To(Equal(codes.Error))
			Expect(s.Status.Description).To(Equal("connection refused"))
			var hasException bool
			for _, e := range s.Events {
				if e.Name == "exception" {
					hasException = true
				}
			}
			Expect(hasException).To(BeTrue())
		})
	})

	// -------------------------------------------------------------------------
	Describe("config()", func() {
		var connConfig *pgx.ConnConfig

		BeforeEach(func() {
			var err error
			connConfig, err = pgx.ParseConfig("host=db.example.com port=5433 user=testuser password=secret dbname=testdb")
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns db.system.name = postgresql", func() {
			a := findAttr(qt.config(connConfig), "db.system.name")
			Expect(a).NotTo(BeNil())
			Expect(a.Value.AsString()).To(Equal("postgresql"))
		})

		It("returns db.namespace from ConnConfig.Database", func() {
			a := findAttr(qt.config(connConfig), "db.namespace")
			Expect(a).NotTo(BeNil())
			Expect(a.Value.AsString()).To(Equal("testdb"))
		})

		It("returns server.address from ConnConfig.Host", func() {
			a := findAttr(qt.config(connConfig), "server.address")
			Expect(a).NotTo(BeNil())
			Expect(a.Value.AsString()).To(Equal("db.example.com"))
		})

		It("returns server.port from ConnConfig.Port", func() {
			a := findAttr(qt.config(connConfig), "server.port")
			Expect(a).NotTo(BeNil())
			Expect(a.Value.AsInt64()).To(Equal(int64(5433)))
		})

		It("does not include db.user or db.connection_string", func() {
			attrs := qt.config(connConfig)
			Expect(findAttr(attrs, "db.user")).To(BeNil())
			Expect(findAttr(attrs, "db.connection_string")).To(BeNil())
		})
	})

	// -------------------------------------------------------------------------
	Describe("operationName()", func() {
		DescribeTable("maps CommandTag to db.operation.name",
			func(tagStr, expectedOp string) {
				kv := qt.operationName(pgconn.NewCommandTag(tagStr))
				Expect(string(kv.Key)).To(Equal("db.operation.name"))
				Expect(kv.Value.AsString()).To(Equal(expectedOp))
			},
			Entry("SELECT", "SELECT 5", "SELECT"),
			Entry("INSERT", "INSERT 0 1", "INSERT"),
			Entry("UPDATE", "UPDATE 3", "UPDATE"),
			Entry("DELETE", "DELETE 2", "DELETE"),
			Entry("COPY → UNKNOWN", "COPY 10", "UNKNOWN"),
			Entry("empty → UNKNOWN", "", "UNKNOWN"),
		)
	})

	// -------------------------------------------------------------------------
	Describe("collection()", func() {
		DescribeTable("returns db.collection.name with sanitized identifier",
			func(parts []string, expected string) {
				kv := qt.collection(pgx.Identifier(parts))
				Expect(string(kv.Key)).To(Equal("db.collection.name"))
				Expect(kv.Value.AsString()).To(Equal(expected))
			},
			Entry("single part", []string{"users"}, `"users"`),
			Entry("schema qualified", []string{"public", "orders"}, `"public"."orders"`),
		)
	})

	// -------------------------------------------------------------------------
	Describe("sanitizeSQL()", func() {
		DescribeTable("strips comments and joins lines",
			func(input, expected string) {
				Expect(sanitizeSQL(input)).To(Equal(expected))
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
			Entry("multi-line SQL joined",
				"SELECT id\nFROM users\nWHERE id = $1",
				"SELECT id FROM users WHERE id = $1",
			),
			Entry("-- name: comment stripped, SQL preserved",
				"-- name: FindUser\nSELECT * FROM users WHERE id = $1",
				"SELECT * FROM users WHERE id = $1",
			),
			Entry("all-comment query returns empty string",
				"-- just a comment\n-- another comment",
				"",
			),
			Entry("empty lines ignored",
				"SELECT id\n\n   \nFROM users",
				"SELECT id FROM users",
			),
		)
	})

	// -------------------------------------------------------------------------
	Describe("queryText()", func() {
		It("returns nil when IncludeStatement is false (default)", func() {
			Expect(qt.queryText("SELECT 1")).To(BeNil())
		})

		It("returns db.query.text with sanitized SQL when IncludeStatement is true", func() {
			qt.IncludeStatement = true
			kv := qt.queryText("-- name: Foo\nSELECT 1 -- inline")
			Expect(kv).NotTo(BeNil())
			Expect(string(kv.Key)).To(Equal("db.query.text"))
			Expect(kv.Value.AsString()).To(Equal("SELECT 1"))
		})
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
			result := qt.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: connConfig})
			Expect(result).To(Equal(ctx))
			Expect(exporter.GetSpans()).To(BeEmpty())
		})

		It("recording parent creates a child span named Connect with SpanKindClient", func() {
			ctx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
			defer parentSpan.End()

			ctx = qt.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: connConfig})
			qt.TraceConnectEnd(ctx, pgx.TraceConnectEndData{})

			s := findSpanByName(exporter.GetSpans(), "Connect")
			Expect(s).NotTo(BeNil())
			Expect(s.SpanKind).To(Equal(trace.SpanKindClient))
		})

		It("span carries server.address and db.namespace attributes", func() {
			ctx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
			defer parentSpan.End()

			ctx = qt.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: connConfig})
			qt.TraceConnectEnd(ctx, pgx.TraceConnectEndData{})

			s := findSpanByName(exporter.GetSpans(), "Connect")
			Expect(s).NotTo(BeNil())
			Expect(findAttr(s.Attributes, "server.address")).NotTo(BeNil())
			Expect(findAttr(s.Attributes, "db.namespace")).NotTo(BeNil())
			Expect(findAttr(s.Attributes, "db.connection_string")).To(BeNil())
		})

		It("TraceConnectEnd with nil error → codes.Unset", func() {
			ctx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
			defer parentSpan.End()

			ctx = qt.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: connConfig})
			qt.TraceConnectEnd(ctx, pgx.TraceConnectEndData{Err: nil})

			s := findSpanByName(exporter.GetSpans(), "Connect")
			Expect(s).NotTo(BeNil())
			Expect(s.Status.Code).To(Equal(codes.Unset))
		})

		It("TraceConnectEnd with error → codes.Error + description", func() {
			ctx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
			defer parentSpan.End()

			connErr := errors.New("dial tcp: connection refused")
			ctx = qt.TraceConnectStart(ctx, pgx.TraceConnectStartData{ConnConfig: connConfig})
			qt.TraceConnectEnd(ctx, pgx.TraceConnectEndData{Err: connErr})

			s := findSpanByName(exporter.GetSpans(), "Connect")
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
			if os.Getenv("PGX_DATABASE_URL") == "" {
				Skip("PGX_DATABASE_URL not set")
			}

			config, err := pgxpool.ParseConfig(os.Getenv("PGX_DATABASE_URL"))
			Expect(err).NotTo(HaveOccurred())

			// Use a dedicated tracer with IncludeStatement so integration
			// tests can verify db.query.text without disturbing unit tests.
			config.ConnConfig.Tracer = &QueryTracer{
				Name:             "integration-tracer",
				IncludeStatement: true,
			}

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
			It("creates a low-cardinality span named after the SQL keyword", func() {
				rows, err := pool.Query(intCtx, "SELECT 1")
				Expect(err).NotTo(HaveOccurred())
				rows.Close()
				intSpan.End()

				Expect(findSpanByName(exporter.GetSpans(), "SELECT")).NotTo(BeNil())
			})

			It("uses -- name: comment as span name", func() {
				rows, err := pool.Query(intCtx, "-- name: HelloWorld\nSELECT 1")
				Expect(err).NotTo(HaveOccurred())
				rows.Close()
				intSpan.End()

				Expect(findSpanByName(exporter.GetSpans(), "HelloWorld")).NotTo(BeNil())
			})

			It("db.query.text has comments stripped", func() {
				rows, err := pool.Query(intCtx, "-- name: Stripped\nSELECT 42")
				Expect(err).NotTo(HaveOccurred())
				rows.Close()
				intSpan.End()

				s := findSpanByName(exporter.GetSpans(), "Stripped")
				Expect(s).NotTo(BeNil())
				stmt := findAttr(s.Attributes, "db.query.text")
				Expect(stmt).NotTo(BeNil())
				Expect(stmt.Value.AsString()).NotTo(ContainSubstring("--"))
				Expect(stmt.Value.AsString()).To(ContainSubstring("SELECT 42"))
			})

			It("db.operation.name is set at query end", func() {
				rows, err := pool.Query(intCtx, "SELECT 1")
				Expect(err).NotTo(HaveOccurred())
				rows.Close()
				intSpan.End()

				s := findSpanByName(exporter.GetSpans(), "SELECT")
				Expect(s).NotTo(BeNil())
				op := findAttr(s.Attributes, "db.operation.name")
				Expect(op).NotTo(BeNil())
				Expect(op.Value.AsString()).To(Equal("SELECT"))
			})

			It("pgx.ErrNoRows does NOT set codes.Error on the span", func() {
				var val int
				err := pool.QueryRow(intCtx, "SELECT 1 WHERE 1=0").Scan(&val)
				Expect(errors.Is(err, pgx.ErrNoRows)).To(BeTrue())
				intSpan.End()

				for _, s := range exporter.GetSpans() {
					if findAttr(s.Attributes, "db.namespace") != nil {
						Expect(s.Status.Code).NotTo(Equal(codes.Error))
					}
				}
			})
		})

		Describe("TracePrepareStart/End", func() {
			It("creates a span with SpanKindClient and db.query.text", func() {
				conn, err := pool.Acquire(intCtx)
				Expect(err).NotTo(HaveOccurred())
				defer conn.Release()

				_, err = conn.Conn().Prepare(intCtx, "stmt1", "SELECT $1::int")
				Expect(err).NotTo(HaveOccurred())
				intSpan.End()

				s := findSpanByName(exporter.GetSpans(), "SELECT")
				Expect(s).NotTo(BeNil())
				Expect(s.SpanKind).To(Equal(trace.SpanKindClient))
				Expect(findAttr(s.Attributes, "db.query.text")).NotTo(BeNil())
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

			It("creates a span named Copy with db.collection.name attribute", func() {
				_, err := pool.CopyFrom(
					intCtx,
					pgx.Identifier{"pgxotel_copy_test"},
					[]string{"id"},
					pgx.CopyFromRows([][]any{{1}, {2}, {3}}),
				)
				Expect(err).NotTo(HaveOccurred())
				intSpan.End()

				s := findSpanByName(exporter.GetSpans(), "Copy")
				Expect(s).NotTo(BeNil())
				tbl := findAttr(s.Attributes, "db.collection.name")
				Expect(tbl).NotTo(BeNil())
				Expect(tbl.Value.AsString()).To(ContainSubstring("pgxotel_copy_test"))
			})
		})

		Describe("TraceBatchStart/BatchQuery/BatchEnd", func() {
			It("creates a BatchStart parent span and per-SQL child spans", func() {
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
				Expect(findSpanByName(spans, "BatchStart")).NotTo(BeNil())

				var batchQueryCount int
				for i := range spans {
					if spans[i].Name == "SELECT" {
						for _, e := range spans[i].Events {
							if e.Name == "BatchQuery" {
								batchQueryCount++
								break
							}
						}
					}
				}
				Expect(batchQueryCount).To(Equal(2))
			})
		})
	})
})
