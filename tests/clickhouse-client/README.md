# clickhouse-client-cli

A simple CLI tool that mimics `clickhouse-client` for executing SQL queries against a ClickHouse server.
Used to test with ClickHouse test framework designed for `clickhouse-client` (https://github.com/ClickHouse/ClickHouse/blob/master/tests/clickhouse-test). 
Note: do not clone ClickHouse repo - it takes a lot of time. Download zip instead.

## Build Java Application

```bash
cd tests/clickhouse-client
mvn package -DskipTests
```

This produces an executable fat JAR at `target/clickhouse-client-cli-1.0.0.jar`.

## Wrapper executable

A wrapper script named `clickhouse-client` is provided in `bin/` directory. It is a simple shell script that calls 
java application. It is required because `clickhouse-test` script calls `clickhouse-client` binary found in `PATH` environment variable.
It is recommended to set `PATH` locally in terminal session to not override real `clickhouse-client`.

## Environment variables

| Variable | Description |
|---|---|
| `CLICKHOUSE_CLIENT_CLI_IMPL` | Backend implementation to use: `client` (default, uses client-v2 API) or `jdbc` (uses ClickHouse JDBC driver) |
| `CLICKHOUSE_CLIENT_CLI_LOG` | Path to log file for troubleshooting |

## Examples

Run tests using the default client-v2 backend:

```shell
cd ClickHouse-master
CLICKHOUSE_CLIENT_CLI_LOG=./test-run.log PATH="$PATH:/home/someuser/clickhouse-java/tests/clickhouse-client/bin/" tests/clickhouse-test 01428_hash_set_nan_key
```

Run tests using the JDBC backend:

```shell
cd ClickHouse-master
CLICKHOUSE_CLIENT_CLI_IMPL=jdbc CLICKHOUSE_CLIENT_CLI_LOG=./test-run.log PATH="$PATH:/home/someuser/clickhouse-java/tests/clickhouse-client/bin/" tests/clickhouse-test 01428_hash_set_nan_key
```