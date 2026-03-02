# clickhouse-client-cli

A simple CLI tool that mimics `clickhouse-client` for executing SQL queries against a ClickHouse server.
Uses the Java client-v2 API over HTTP, requests `TabSeparated` format, and streams raw output to stdout.

## Build

```bash
cd tests/clickhouse-client
mvn package -DskipTests
```

This produces an executable fat JAR at `target/clickhouse-client-cli-1.0.0.jar`.

## Wrapper executable

A wrapper script named `clickhouse-client` is provided in this directory.

```bash
cd tests/clickhouse-client
./clickhouse-client --help
```

To call it as `clickhouse-client` from anywhere:

```bash
export PATH="$PATH:/home/schernov/workspace01/clickhouse-java/tests/clickhouse-client"
clickhouse-client --help
```

## Usage

Both `--option value` and `--option=value` formats are supported.

### Query via `--query` / `-q`

```bash
./clickhouse-client -q "SELECT uniqExact(number) FROM numbers(1000)"
```

### Query via stdin

Pipe a query:

```bash
echo "SELECT uniqExact(number) FROM numbers(1000)" | ./clickhouse-client
```

Here-string:

```bash
./clickhouse-client <<< "SELECT 1"
```

From a file:

```bash
./clickhouse-client < query.sql
```

If no `--query` is given and nothing is piped, the process blocks waiting for input.
Type your SQL and press `Ctrl+D` (EOF) to execute.

## Options

| Option             | Default     | Description              |
|--------------------|-------------|--------------------------|
| `--host`, `-h`     | `localhost` | Server host              |
| `--port`           | `8123`      | HTTP port                |
| `--user`, `-u`     | `default`   | Username                 |
| `--password`       | *(empty)*   | Password                 |
| `--database`, `-d` | `default`   | Database                 |
| `--query`, `-q`    |             | SQL query to execute     |
| `--log_comment`    |             | Comment for query log    |
| `--send_logs_level`|             | Send server logs level   |
| `--max_insert_threads` |         | Server setting passthrough |
| `--multiquery`     |             | Execute `;`-separated SQL statements |
| `--secure`, `-s`   | off         | Use HTTPS                |
| `--multiline`, `-n`|             | Ignored (compatibility)  |
| `--help`           |             | Print usage              |

Unknown long options in the form `--name value` / `--name=value` are also accepted and forwarded as ClickHouse server settings.

## Examples

```bash
# simple select
./clickhouse-client -q "SELECT 1"

# connect to a remote server with credentials
./clickhouse-client \
  --host ch.example.com --port 8443 --secure \
  --user admin --password secret \
  --log_comment "sync-job-42" \
  --send_logs_level warning \
  -q "SELECT count() FROM system.tables"

# multi-line query from stdin
./clickhouse-client <<'EOF'
SELECT
    database,
    count() AS table_count
FROM system.tables
GROUP BY database
ORDER BY table_count DESC
EOF

# multiquery from stdin (queries separated by ;)
./clickhouse-client --multiquery <<'EOF'
CREATE TEMPORARY TABLE t (x UInt8);
INSERT INTO t VALUES (1), (2), (3);
SELECT sum(x) FROM t;
EOF
```
