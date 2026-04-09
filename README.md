# pgstream

A lightweight CLI tool for tailing and filtering PostgreSQL WAL changes in real time with pluggable output formats.

## Features

- 🔄 Real-time streaming of PostgreSQL logical replication changes
- 🎯 Filter by table, schema, or operation type (INSERT/UPDATE/DELETE)
- 📝 Multiple output formats: JSON, pretty-print, CSV
- ⚡ Minimal overhead and memory footprint
- 🔌 Pluggable architecture for custom formatters

## Installation

```bash
go install github.com/yourusername/pgstream@latest
```

Or download pre-built binaries from the [releases page](https://github.com/yourusername/pgstream/releases).

## Usage

```bash
# Stream all changes from a PostgreSQL database
pgstream --host localhost --port 5432 --database mydb --user postgres

# Filter by table and output as JSON
pgstream --dsn "postgres://user:pass@localhost/mydb" \
  --table users \
  --format json

# Filter by operation type
pgstream --dsn "postgres://user:pass@localhost/mydb" \
  --operations INSERT,UPDATE \
  --schema public

# Pretty-print output with color
pgstream --dsn "postgres://user:pass@localhost/mydb" \
  --format pretty \
  --color
```

## Configuration

PostgreSQL must be configured with `wal_level = logical`. See the [setup guide](docs/setup.md) for detailed configuration instructions.

## Output Formats

- `json` - One JSON object per line
- `pretty` - Human-readable colored output
- `csv` - Comma-separated values

## License

MIT License - see [LICENSE](LICENSE) for details.