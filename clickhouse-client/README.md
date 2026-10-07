# ClickHouse Java Client

Async Java client for ClickHouse. `clickhouse-client` is an abstract module, so it does not work by itself until being used together with an implementation like `clickhouse-http-client`, `clickhouse-grpc-client` or `clickhouse-cli-client`.

## Documentation
See the [ClickHouse website](https://clickhouse.com/docs/en/integrations/language-clients/java/client) for the full documentation entry.

## Configuration

You can pass any client option([common](https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-client/src/main/java/com/clickhouse/client/config/ClickHouseClientOption.java), [http](https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-http-client/src/main/java/com/clickhouse/client/http/config/ClickHouseHttpOption.java), [grpc](https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-grpc-client/src/main/java/com/clickhouse/client/grpc/config/ClickHouseGrpcOption.java), and [cli](https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-cli-client/src/main/java/com/clickhouse/client/cli/config/ClickHouseCommandLineOption.java)) to `ClickHouseRequest.option()` and [server setting](https://clickhouse.com/docs/en/operations/settings/) to `ClickHouseRequest.set()` before execution, for instance:

```java
client.connect("http://localhost/system")
    .query("select 1")
    // short version of option(ClickHouseClientOption.FORMAT, ClickHouseFormat.RowBinaryWithNamesAndTypes)
    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
    .option(ClickHouseClientOption.SOCKET_TIMEOUT, 30000 * 2) // 60 seconds
    .set("max_rows_to_read", 100)
    .set("read_overflow_mode", "throw")
    .execute()
    .whenComplete((response, throwable) -> {
        if (throwable != null) {
            log.error("Unexpected error", throwable);
        } else {
            try {
                for (ClickHouseRecord rec : response.records()) {
                    // ...
                }
            } finally {
                response.close();
            }
        }
    });
```

[Default value](https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-client/src/main/java/com/clickhouse/client/config/ClickHouseDefaults.java) can be either configured via system property or environment variable.

## Examples
For more example please check [here](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/client).

## Test Data Generator

The test sources contain a generator of test data sets (`com.clickhouse.client.testdata`) covering boundary,
edge-case and random values of ClickHouse data types. `GenerateTestDataSet` writes such a data set to a file,
with column types selected on the command line.

Run it from the repository root with Maven:

```bash
mvn -q -pl clickhouse-client test-compile exec:java -Dexec.classpathScope=test \
    -Dexec.mainClass=com.clickhouse.client.testdata.GenerateTestDataSet \
    -Dexec.args='-o data.sql Int32 "s Nullable(String)" "dt DateTime64(3)"'
```

Relative output paths are resolved against the directory `mvn` runs in. Types with quoted
arguments, like `DateTime64(3, 'Asia/Kolkata')`, are easier to pass with plain Java.

After `mvn -pl clickhouse-client test-compile` it can also run directly with Java (the generator only needs the JDK):

```bash
java -cp clickhouse-client/target/test-classes com.clickhouse.client.testdata.GenerateTestDataSet \
    -o data.sql Int32 "s Nullable(String)" "dt DateTime64(3, 'Asia/Kolkata')" "m Map(String, Array(UInt8))"
```

Each column is a ClickHouse column definition `"name Type"`, or just `Type` (named `c1`, `c2`, ...). Every
data set also has an `id Int32` column with the row number. The number of rows is defined by the generated
values: the column with most values wins and other columns repeat their values cyclically.

| Option | Description |
| --- | --- |
| `-o`, `--output <file>` | output file, stdout by default |
| `-f`, `--format <format>` | `sql` (default) - `SET` statements for required settings, `CREATE TABLE` and `INSERT`; `values` - rows as SQL tuples only; `tsv` - rows in the `TabSeparated` format, exactly as ClickHouse writes them |
| `-t`, `--table <name>` | table name for the `sql` format, `test_data` by default |
| `-s`, `--seed <long>` | random seed; the same seed produces the same data |
| `-h`, `--help` | print usage and all supported types |

Supported types:

- `Int8`..`Int256`, `UInt8`..`UInt256`, `Float32`, `Float64`, `Bool`
- `Decimal(P[, S])`, `Decimal32(S)`, `Decimal64(S)`, `Decimal128(S)`, `Decimal256(S)`
- `String`, `FixedString(N)`, `BinaryString` (not a ClickHouse type: a `String` column with arbitrary bytes)
- `Date`, `Date32`, `Time`, `Time64(P)`, `DateTime[('tz')]`, `DateTime64(P[, 'tz'])` (timezone defaults to `UTC`)
- `IPv4`, `IPv6`, `UUID`, `Enum8`, `Enum16` (with built-in constants) or `Enum8('a' = 1, ...)`
- `Point`, `Ring`, `LineString`, `Polygon`, `MultiLineString`, `MultiPolygon`
- `Array(T)`, `Tuple(T, ...)`, `Tuple(name T, ...)`, `Map(K, V)`, `Nested(name T, ...)`, `Nullable(T)`, `LowCardinality(T)`
- `Interval<Unit>` and `Nothing` - cannot be stored in a table, so only with `-f values` or `-f tsv`

The `sql` output can be loaded with `clickhouse client --multiquery < data.sql`.

### Pre-generated data sets

`src/test/resources/datasets` contains a data set for every supported type, with the data in a single
`value` column. Files are named after the type, for example `array_nullable_string.*` for
`Array(Nullable(String))`. Every data set has two files with the same rows:

- `<type>.sql` - `SET` statements for required settings, `CREATE TABLE test_data` and `INSERT ... VALUES`.
  Interval and `Nothing` types cannot be stored in a table, so for them it is `<type>.txt` with the rows as
  SQL tuples only.
- `<type>.tsv` - the rows in the `TabSeparated` format, byte for byte what ClickHouse returns for
  `SELECT * FROM test_data ORDER BY id FORMAT TSV` after running `<type>.sql`. Use it to verify how a client
  reads or writes the values.

The TSV text follows the server defaults, for example `output_format_decimal_trailing_zeros = 0`. In Java the
same text is available from `TestDataSet.getTsv()`.

To regenerate them after changing the generators, or to add a type to the `STORABLE_TYPES` or
`VALUES_ONLY_TYPES` lists in the script, run:

```bash
clickhouse-client/scripts/generate-datasets.sh              # builds test classes first
clickhouse-client/scripts/generate-datasets.sh --skip-build # uses already compiled test classes
```

The script replaces all `.sql`, `.txt` and `.tsv` files in the directory. With the default seed the output is
reproducible, so a diff of the regenerated files shows exactly what changed in the generated values.
