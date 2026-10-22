
# JMH benchmarks

The `performance` module is a standalone Java 11 JMH project. Build the
repository artifacts it benchmarks before packaging the module:

```shell
mvn -DskipTests -pl packages/clickhouse-jdbc-all -am install
mvn -f performance/pom.xml clean package
```

The shipping libraries remain Java 8 compatible; Java 11 is required only to
compile and run this benchmark module.

## How to run


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

### Write-stream benchmark

`WriteStream` isolates the HTTP request-body handoff from serialization and
server storage. Each invocation sends the same 25,000 pre-serialized
`FixedString(256)` RowBinary rows (6.4 MB) to an `ENGINE = Null` table. Three
benchmark threads model three concurrent sink tasks. The methods are:

- `v1NonBlocking`: the default V1 callback stream and its non-blocking queue;
- `v1BlockingQueue`: the same V1 path with only `use_blocking_queue` changed;
- `v2Direct`: the V2 callback stream.

This is a mechanism benchmark, not an application or connector benchmark. It
deliberately excludes conversion, batching policy, and RowBinary serialization.
Use it to compare the three request-body paths under the same conditions, not
to predict production throughput.

From the repository root, run the CPU-capped benchmark and profiler:

```shell
mkdir -p performance/results/write-stream
GIT_COMMIT="$(git rev-parse HEAD)" \
  docker compose -f performance/compose.write-stream.yml up \
  --build --abort-on-container-exit --exit-code-from benchmark benchmark
docker compose -f performance/compose.write-stream.yml down
```

The benchmark container has a 3.5-CPU CFS quota and a 6 GiB heap. ClickHouse
runs in a separate container, and the Null table removes persistence and merge
work. The run writes these files under `performance/results/write-stream`:

- `jmh-results.json` and `jmh-results.out`;
- one `cpu-<pid>.html` async-profiler flame graph per JMH fork.

JMH reports rows/second, not batches/second, because each invocation is
declared as 25,000 operations. The Docker setup intentionally uses
async-profiler's `itimer` event: it samples on-CPU time without requiring
host `perf_event` permissions.

Interpret profiles as evidence to test a mechanism, not as proof by themselves.
In particular, compare `v1NonBlocking` with `v1BlockingQueue` to isolate queue
waiting policy while retaining the V1 transport, and compare `v1BlockingQueue`
with `v2Direct` to identify overhead that remains after that control.

### Baseline and environment methodology

Performance claims must compare a baseline commit with a candidate commit. Use
separate clean worktrees and the same commands for both:

1. Run on the same otherwise-idle host. Keep CPU governor, Docker version,
   architecture, kernel, JVM, ClickHouse image, CPU quota, heap, thread count,
   warmup, and measurement settings unchanged.
2. Record the commit SHA and `git status --short`, `uname -a`, CPU model and
   count, available memory, `java -version`, `mvn -version`, `docker version`,
   and `docker compose version` beside every raw result.
3. Alternate baseline and candidate runs to reduce time-order bias. Use at
   least three independent process launches per commit for a decision; do not
   treat extra JMH iterations in one fork as independent runs.
4. Retain the JSON, text output, environment record, and profiler output from
   every run. Do not edit, average, or copy point estimates into the repository.
5. Define invalid-run criteria before running (for example, host contention,
   thermal throttling, or failed setup). Report every exclusion and compare
   uncertainty as well as point estimates.

The compose build records `GIT_COMMIT` in the packaged libraries; it uses
`unknown` if the variable is omitted. The compose file pins the ClickHouse
image tag and benchmark resource limits. For long-lived baselines, also record
the resolved image digest because tags can be republished.

Compare matching results without adding a Python dependency:

```shell
python3 performance/compare-jmh.py \
  baseline/jmh-results.json candidate/jmh-results.json \
  > comparison.csv
```

The utility emits the candidate-to-baseline ratio and percent change for the
JMH primary-metric point estimates. A ratio above one is better for throughput
and worse for time-based modes. The CSV is a convenience for inspection; use
the raw JMH uncertainty and independent-run distribution before drawing a
conclusion.

### Running benchmarks

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
- "-l" - comma-separated dataset limits to test (for example, `-l 10000,10000`)
- "-m" - number of measurement iterations
- "-t" - time in seconds per iteration
- "-b" - comma-separated benchmark mask. Example: `-b writer,reader,i`. Default: `-b i,q`
  - "all" - run all general benchmarks
  - "i" - InsertClient - insert operation benchmarks
  - "q" - QueryClient - query operation benchmarks
  - "ci" - ConcurrentInsertClient - concurrent version of insert benchmarks
  - "cq" - ConcurrentQueryClient - concurrent version of query benchmarks
  - "lz" - Compression - LZ4 output stream benchmarks (no server involved)
  - "comp" - Compression - query/insert compression matrix of clients, methods, algorithms and formats (see below)
  - "writer" - Serializer - serialization only logic benchmarks
  - "reader" - Deserializer - deserialization-only logic benchmarks
  - "mixed" - MixedWorkload
  - "jq" - JDBCQuery - query operations using JDBC
  - "ji" - JDBCInsert - insert operation using JDBC
  - "ws" - WriteStream - isolated V1/V2 HTTP request-body handoff

`ws` uses throughput mode and must be selected by itself. It is excluded from
`-b all`; run it explicitly with `-b ws`.

Compression matrix filters (used with `-b comp` or `-b all`, each defaults to all values):
- "-cc" - clients: `v1,v2`
- "-cm" - compression methods: `http` (`Content-Encoding`/`Accept-Encoding`), `native` (ClickHouse block compression, `use_http_compression = false`)
- "-ca" - algorithms: `lz4,zstd,snappy,brotli`
- "-cf" - formats: `RowBinaryWithNamesAndTypes,JSONEachRow` (any ClickHouse format name, e.g. `RowBinary`, is accepted)

Ex.: `-b comp -cc v2 -cm http -ca zstd,lz4 -cf JSONEachRow`.

Combinations a client doesn't support are skipped: V1 sends LZ4 only natively and other algorithms only over HTTP,
V2 native mode is LZ4 only, V2 can't compress requests with brotli, and snappy works with neither client.