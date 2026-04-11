# pgxotel

[![CI](https://github.com/pgx-contrib/pgxotel/actions/workflows/ci.yml/badge.svg)](https://github.com/pgx-contrib/pgxotel/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/pgx-contrib/pgxotel?include_prereleases)](https://github.com/pgx-contrib/pgxotel/releases)
[![License](https://img.shields.io/github/license/pgx-contrib/pgxotel)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![pgx](https://img.shields.io/badge/pgx-v5-blue)](https://github.com/jackc/pgx)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-enabled-blueviolet)](https://opentelemetry.io)

OpenTelemetry tracing for [pgx v5](https://github.com/jackc/pgx). Attach
`QueryTracer` to any `pgx.ConnConfig` or `pgxpool.Config` and every database
operation — queries, batch sends, prepared statements, COPY, and connections —
is automatically recorded as an OpenTelemetry span.

## Features

- Instruments all five pgx tracer interfaces: `QueryTracer`, `BatchTracer`,
  `ConnectTracer`, `PrepareTracer`, and `CopyFromTracer`
- Low-cardinality span names: extracts the first SQL keyword (`SELECT`,
  `INSERT`, …) or uses an explicit `-- name: Foo` comment
- Follows stable OpenTelemetry semantic conventions (`semconv/v1.40.0`):
  `db.namespace`, `db.operation.name`, `db.query.text`, `db.collection.name`,
  `server.address`, `server.port`
- `db.query.text` is **opt-in** (`IncludeStatement: true`) to avoid recording
  sensitive SQL structure by default
- Accepts an optional `TracerProvider` for scoped, test-friendly setup; falls
  back to the global provider when unset

## Installation

```bash
go get github.com/pgx-contrib/pgxotel
```

## Usage

### Connection pool

```go
config, err := pgxpool.ParseConfig(os.Getenv("PGX_DATABASE_URL"))
if err != nil {
    panic(err)
}

config.ConnConfig.Tracer = &pgxotel.QueryTracer{
    Name: "my-service",
}

pool, err := pgxpool.NewWithConfig(context.Background(), config)
if err != nil {
    panic(err)
}
defer pool.Close()

rows, err := pool.Query(context.Background(), "SELECT * FROM customer")
if err != nil {
    panic(err)
}
defer rows.Close()
```

### Named queries

Prefix any SQL string with `-- name: <Identifier>` to control the span name.
The comment is stripped from `db.query.text` automatically:

```go
rows, err := pool.Query(ctx,
    "-- name: ListActiveCustomers\nSELECT * FROM customer WHERE active = true",
)
```

This produces a span named `ListActiveCustomers` instead of `SELECT`.

### Recording SQL text

SQL is not recorded by default. Enable it per-tracer when the query text is not
considered sensitive:

```go
config.ConnConfig.Tracer = &pgxotel.QueryTracer{
    Name:             "my-service",
    IncludeStatement: true,
}
```

### Scoped TracerProvider

```go
config.ConnConfig.Tracer = &pgxotel.QueryTracer{
    Name:     "my-service",
    Provider: myTracerProvider,
}
```

## Span attributes

Every span carries the following attributes:

| Attribute            | Value                                                 |
| -------------------- | ----------------------------------------------------- |
| `db.system.name`     | `postgresql`                                          |
| `db.namespace`       | database name                                         |
| `server.address`     | host                                                  |
| `server.port`        | port                                                  |
| `db.operation.name`  | `SELECT` / `INSERT` / `UPDATE` / `DELETE` / `UNKNOWN` |
| `db.collection.name` | table name (CopyFrom only)                            |
| `db.query.text`      | sanitized SQL (opt-in via `IncludeStatement`)         |

## Contributing

Contributions are welcome! Please open an issue or pull request.

To set up a development environment with [Nix](https://nixos.org):

```bash
nix develop
```

Or using the provided dev container:

```bash
devcontainer up --workspace-folder . --remove-existing-container
```

Then run the tests:

```bash
go test ./...
```

## License

[MIT](LICENSE)
