
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

### Reproducing the V1 write-stream spin

`WriteStream` isolates the HTTP request-body handoff from serialization and
server storage. Each invocation sends the same 25,000 pre-serialized
`FixedString(256)` RowBinary rows (6.4 MB) to an `ENGINE = Null` table. Three
benchmark threads model three concurrent sink tasks. The methods are:

- `v1NonBlocking`: the default V1 callback stream and its non-blocking queue;
- `v1BlockingQueue`: the same V1 path with only `use_blocking_queue` changed;
- `v2Direct`: the V2 callback stream.

From the repository root, run the CPU-capped benchmark and profiler with:

```shell
mkdir -p performance/results/write-stream
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

#### Why V1 spends CPU while waiting

A V1 `.data(writer)` request does not write the callback's bytes directly to
the HTTP entity output stream. It creates a producer/consumer handoff:

1. `DelegatedInputStream` runs the writer callback on a `ClickHouseWorker`
   thread and writes buffers to `NonBlockingPipedOutputStream`.
2. The request thread enters `ClickHouseHttpEntity.writeTo`, then
   `ClickHouseHttpConnection.postData`, and drains the corresponding input
   stream through `ClickHouseInputStream.pipe`.
3. `NonBlockingInputStream.updateBuffer` repeatedly calls
   `AdaptiveQueue.DefaultQueue.poll` until the producer publishes another
   buffer. When a timeout is configured, every failed poll also calls
   `System.currentTimeMillis`.

There is no park, wait, or backoff in that loop. With three inserts in flight,
the producer threads do useful work while three consumers can remain runnable
and consume their CPU quota polling empty queues. The expected V1 flame graph
therefore contains the following cluster:

```text
[vdso] / __clock_gettime
  <- System.currentTimeMillis
  <- NonBlockingInputStream.updateBuffer
  <- ClickHouseInputStream.pipe
  <- ClickHouseHttpConnection.postData
  <- ClickHouseHttpEntity.writeTo

AdaptiveQueue$DefaultQueue.poll
  <- NonBlockingInputStream.updateBuffer
  <- ClickHouseInputStream.pipe
  <- ClickHouseHttpConnection.postData
  <- ClickHouseHttpEntity.writeTo
```

The blocking V1 method is a causal control. It retains the V1 HTTP and piped
handoff but replaces the spin-poll with a blocking queue. Its throughput and
profile show how much of the default V1 result comes from waiting policy rather
than payload size, HTTP, or ClickHouse.

The blocking control is not expected to reach V2 throughput. It still pays for
the producer thread, queue synchronization, intermediate buffers, and the pipe
copy. Compare non-blocking V1 with blocking V1 to isolate the wait policy;
compare blocking V1 with V2 to see the remaining handoff overhead.

V2 does not optimize that loop; it removes the handoff from the insert path.
`HttpAPIClientHelper` gives Apache HttpClient 5 an `EntityTemplate` whose
`IOCallback<OutputStream>` writes the body directly to the connection output
stream. Consequently, the V2 profile should have no `NonBlocking*`,
`AdaptiveQueue`, or `ClickHouseInputStream.pipe` request-body frames.

The CPU quota is essential to interpreting throughput. Without a quota, V1 can
burn otherwise idle cores and finish at roughly the same rate, making the
wasted CPU visible only in a profile. Under a quota, spin time displaces useful
work and lowers rows/second.

This is a mechanism benchmark, not a Kafka Connect benchmark. It deliberately
excludes Avro conversion, Schema Registry, connector batching, and real
RowBinary serialization. It should reproduce and attribute the capped-CPU
penalty, but the exact V1/V2 ratio is hardware- and scheduling-dependent and is
not expected to equal 2.5 on every host.

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
  - "lz" - Compression - LZ4 output stream benchmarks (no server involved)
  - "comp" - Compression - query/insert compression matrix of clients, methods, algorithms and formats (see below)
  - "writer" - Serializer - serialization only logic benchmarks
  - "reader" - DeSerilalizer - deserialization only logic benchmarks
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