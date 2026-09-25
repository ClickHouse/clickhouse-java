
## JMH Benchmarks


### Dependencies 



### How to Run


#### Generating Dataset

```shell
mvn compile exec:exec -Dexec.executable=java -Dexec.args="-classpath %classpath com.clickhouse.benchmark.data.DataSetGenerator \
-input sample_dataset.sql -name default -rows 10"
```

#### Target Server

By default the benchmarks start a local ClickHouse Docker container and run
against it. To run against an existing remote server instead (ClickHouse Cloud
or any self-hosted instance), set `CLICKHOUSE_URL`:

```shell
export CLICKHOUSE_URL="https://default:my-password@abc123.clickhouse.cloud:8443?cluster=true"
mvn compile exec:exec
```

```shell
export CLICKHOUSE_URL="http://default@localhost:8123"
mvn compile exec:exec
```

The URL is parsed as: `<scheme>://<username>[:<password>]@<host>[:<port>][?cluster=true]`
- scheme (`http`/`https`) selects whether SSL is used
- username/password come from the URL's user-info; if the password is omitted
  the client connects without one, and if the username is omitted it defaults
  to `default`
- port is optional; if omitted it defaults to `8443` for `https` and `8123`
  for `http`
- `cluster` is optional (default `false`) and tells the benchmarks whether the
  remote server is part of a replicated cluster (e.g. ClickHouse Cloud). When
  `true`, a `SYSTEM SYNC REPLICA` is issued after writes. Leave it unset for a
  plain standalone remote server — its tables aren't replicated and it will
  reject that statement.

When `CLICKHOUSE_URL` is unset, the local Docker container is started
automatically and no other configuration is needed.

#### Running Benchmarks 
 
With default settings :
```shell
mvn compile exec:exec
```

With custom measurement iterations: 
```shell
mvn compile exec:exec -Dexec.executable=java -Dexec.args="-classpath %classpath com.clickhouse.benchmark.BenchmarkRunner -m 3"
```

Other options:
- "-d" - dataset name or file path (like `file://default.csv`)
- "-l" - dataset limits to test coma separated (ex.: `-l 10000,10000`)
- "-m" - number of measurement iterations
- "-t" - time in seconds per iteration
- "-b" - benchmark mask coma separated. Ex.: `-b writer,reader,i`. Default : `-b i,q`
  - "all" - Run alpl benchmarks
  - "i" - InsertClient - insert operation benchmarks
  - "q" - QueryClient - query operation benchmarks
  - "ci" - ConcurrentInsertClient - concurrent version of insert benchmarks
  - "cq" - ConcurrentQueryClient - concurrent version of query benchmarks
  - "lz" - Compression - compression related benchmarks
  - "writer" - Serializer - serialization only logic benchmarks
  - "reader" - DeSerilalizer - deserialization only logic benchmarks
  - "mixed" - MixedWorkload 
  - "jq" - JDBCQuery - query operations using JDBC 
  - "ji" - JDBCInsert - insert operation using JDBC