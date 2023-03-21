# ClickHouse Data Processing Utilities

`clickhouse-data` aims to be a handy toolkit for processing data in various formats. It's suitable for producing or consuming data without having to communicate with ClickHouse.

## Quick Start

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-data</artifactId>
    <version>0.4.2</version>
</dependency>
```

```java
/*** serialization/deserialization ***/
ClickHouseDataConfig config = ...;
List<ClickHouseColumn> columns = ClickHouseColumn.parse("a String, b Int32");
ByteArrayOutputStream out = new ByteArrayOutputStream();
try (ClickHouseOutputStream output = ClickHouseOutputStream.of(out)) {
    // find a suitable processor based on config.getFormat()
    ClickHouseDataProcessor processor = ClickHouseDataStreamFactory.getInstance()
            .getProcessor(config, null, output, null, columns);
    // let's reuse same wrapper class for both columns
    ClickHouseValue v = ClickHouseIntegerValue.ofNull();
    for (int i=0; i<50_000; i++) {
        v.update(i);
        for (ClickHouseSerializer s : processor.getSerializers()) {
            s.serialize(v, output);
        }
    }
}
byte[] bytes = out.toByteArray();

/*** type conversion ***/
ClickHouseValue value = ClickHouseStringValue.of("123");
byte b = value.asByte();
int i = value.asInteger();
long l = value.asLong();
value.update(l + i + b); // "369"

/*** streaming ***/
int bufferSize = 8192;
int queueLength = 0; // unlimited
int timeout = 30000; // 30 seconds
ClickHousePipedOutputStream output = ClickHouseDataStreamFactory.getInstance()
        .createPipedOutputStream(bufferSize, queueLength, timeout);
// write and read in separate threads
CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
    try (ClickHouseOuputStream out = output) {
        for (int i = 0; i<50_000; i++) {
            out.writeByte(0)
        }

        return true;
    }
});

// read will be blocked until there's data available in the "pipe"
try (ClickHouseInputStream input = ClickHouseInputStream.of(output.getInputStream(), new FileInputStream("/tmp/mine.txt"))) { // read consolidated input streams
    byte b = input.readByte();
}

/*** compression/decompression ***/
int level = -1;
ClickHouseInputStream input = ClickHouseCompressionAlgorithm.of(ClickHouseCompression.ZSTD)
        .decompress(null, new FileInputStream("/tmp/compressed.data"), bufferSize, level, null);
ClickHouseOutputStream output = ClickHouseCompressionAlgorithm.of(ClickHouseCompression.LZ4)
        .compress(null, new FileInputStream("/tmp/compressed.data"), bufferSize, level, null);
input.pipe(output);
```

## Type Mapping

| Data Type           | Wrapper Class                  | Value Class             | Remark                                          |
| ------------------- | ------------------------------ | ----------------------- | ----------------------------------------------- |
| Bool                | ClickHouseBoolValue            | bool                    |                                                 |
| Date\*              | ClickHouseDateValue            | java.time.LocalDate     |                                                 |
| DateTime\*          | ClickHouseDateTimeValue        | java.time.LocalDateTime | or java.time.OffsetDateTime if there's timezone |
| Enum\*              | ClickHouseEnumValue            | int                     |                                                 |
| FixedString         | ClickHouseStringValue          | byte[]                  |                                                 |
| Int8                | ClickHouseByteValue            | byte                    |                                                 |
| UInt8               | UnsignedByteValue              | byte                    | or short when widen_unsigned_types=true         |
| Int16               | ClickHouseShortValue           | short                   |                                                 |
| UInt16              | UnsignedShortValue             | short                   | or int when widen_unsigned_types=true           |
| Int32               | ClickHouseIntegerValue         | int                     |                                                 |
| UInt32              | UnsignedIntegerValue           | int                     | or long when widen_unsigned_types=true          |
| Int64               | ClickHouseLongValue            | long                    |                                                 |
| UInt64/Interval\*   | UnsignedLongValue              | long                    | or BigInteger when widen_unsigned_types=true    |
| \*Int128            | ClickHouseBigIntegerValue      | BigInteger              |                                                 |
| \*Int256            | ClickHouseBigIntegerValue      | BigInteger              |                                                 |
| Decimal\*           | ClickHouseBigDecimalValue      | BigDecimal              |                                                 |
| Float32             | ClickHouseFloatValue           | float                   |                                                 |
| Float64             | ClickHouseDoubleValue          | double                  |                                                 |
| IPv4                | ClickHouseIpv4Value            | java.net.Inet4Address   |                                                 |
| IPv6                | ClickHouseIpv6Value            | java.net.Inet6Address   |                                                 |
| UUID                | ClickHouseUuidValue            | java.util.UUID          |                                                 |
| Point               | ClickHouseGeoPointValue        | double[2]               |                                                 |
| Ring                | ClickHouseGeoRingValue         | double[][]              |                                                 |
| Polygon             | ClickHouseGeoPolygonValue      | double[][][]            |                                                 |
| MultiPolygon        | ClickHouseGeoMultiPolygonValue | double[][][][]          |                                                 |
| JSON/Object('json') | ClickHouseTupleValue           | java.util.List          |                                                 |
| String              | ClickHouseStringValue          | String                  | or byte[] when use_binary_string=true           |
| Array               | ClickHouseArrayValue           | primitive array         | or Object array when use_objects_in_array=true  |
| Map                 | ClickHouseMapValue             | java.util.Map           |                                                 |
| Nested              | ClickHouseNestedValue          | Object[][]              |                                                 |
| Tuple               | ClickHouseTupleValue           | java.util.List          |                                                 |

All wrapper classes implemented `ClickHouseValue` interface providing the ability to be converted from/to a specific Java type(e.g. via `update(String)` or `asString()`).
