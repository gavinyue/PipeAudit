# PipeAudit

ClickHouse health audit tool written in Rust.

> **Note**: This project was built entirely through AI-assisted coding using Claude Code.

## Features

- **Parts Explosion Detection** - Alerts when tables have too many active parts
- **Merge Backlog Monitoring** - Detects when merges are falling behind
- **Disk Space Warnings** - Monitors disk headroom across storage
- **Query Amplification Analysis** - Identifies queries with high read amplification
- **Stuck Mutation Detection** - Finds mutations running longer than expected
- **MV Dependency Graph** - Maps materialized view relationships

## Usage

```bash
pipeaudit --host localhost --port 8123 --database mydb --tables events,orders
```

### Options

| Flag | Description | Default |
|------|-------------|---------|
| `--host` | ClickHouse host | `localhost` |
| `--port` | ClickHouse HTTP port | `8123` |
| `--user` | Username | `default` |
| `--password` | Password | - |
| `--database` | Database to audit | - |
| `--tables` | Comma-separated table list | - |
| `--output` | Output file path | `report.json` |

## Output

Generates a JSON report with:
- Findings with severity levels (Critical/Warning)
- Recommended actions with SQL commands
- Evidence from system tables
- MV dependency DAG

## Development

```bash
# Run tests
cargo test

# Run with local ClickHouse
docker compose up -d
cargo run -- --database testdb --tables events
```

## License

MIT
