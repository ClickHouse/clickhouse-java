## 0.4.1

### Bug Fixes
* incorrect nested array value. [#1221](https://github.com/ClickHouse/clickhouse-java/issues/1221)
* potential endless loop when handling batch update error. [#1233](https://github.com/ClickHouse/clickhouse-java/issues/1233)

## 0.4.0, 2023-01-19
### Breaking changes
* refactored `JdbcTypeMapping` to make it extensible. [#1075](https://github.com/ClickHouse/clickhouse-java/pull/1075)
* removed legacy driver `ru.yandex.*`.[#1089](https://github.com/ClickHouse/clickhouse-java/pull/1089)
* removed most deprecated methods and class members
* refactored data processor(for serialization and deserialization) and added new classes for unsigned types. [#1124](https://github.com/ClickHouse/clickhouse-java/pull/1124)
* refactored ClickHouseRequest/ClickHouseInputStream/ClickHouseOutputStream to better support compression. [#1174](https://github.com/ClickHouse/clickhouse-java/pull/1174), [#1189](https://github.com/ClickHouse/clickhouse-java/pull/1189)
* extracted `clickhouse-data` from `clickhouse-client` along with new packages. [#1197](https://github.com/ClickHouse/clickhouse-java/pull/1197)
    ```java
    com.clickhouse.config  // generic configuration
    com.clickhouse.data    // data processing utilities
    com.clickhouse.logging // generic logging utility
    ```
* added jdk17 in pom and changed build instructions
    ```bash
    mvn -Dj8 clean install # for jdk8, it was 'mvn clean install'
    mvn clean install # for jdk17, it was 'mvn -Drelease clean install'
    ```
### New Feature
* added R2DBC driver. [#914](https://github.com/ClickHouse/clickhouse-java/pull/914)
* enhanced ClickHouseClient for importing and exporting compressed file. [#1004](https://github.com/ClickHouse/clickhouse-java/pull/1004)
* added new option `custom_settings`. [#1059](https://github.com/ClickHouse/clickhouse-java/pull/1059)
* enhanced `ClickHouseRequestManager` to support query/session ID customization. [#1074](https://github.com/ClickHouse/clickhouse-java/pull/1074)
* added Apache HTTP Client 5 to support socket options. [#1146](https://github.com/ClickHouse/clickhouse-java/pull/1146)
* enhanced `clickhouse-grpc-client` to support request chunking and compression
    * `decompress_alogrithm`(request compression): `BROTLI`[-1,11], `BZ2`, `DEFLATE`[0,9], `GZIP`[-1,9], `LZ4`[0,18], `XZ`[0,9], `ZSTD`[0,22]
    * `compress_alogrithm`(response decompression): `DEFLATE`, `LZ4`, `ZSTD`
    Note: typo will be fixed in v0.4.1.
* enhanced `clickhouse-http-client` to support zstd compression for both request and response
    * `decompress_alogrithm`(request compression): `LZ4`[0,18], `ZSTD`[0,22]
    * `compress_alogrithm`(response decompression): `BROTLI`, `BZ2`, `DEFLATE`, `GZIP`, `LZ4`, `XZ`, `ZSTD`
    Note: typo will be fixed in v0.4.1.
* added stream-based prepared statement. [#1163](https://github.com/ClickHouse/clickhouse-java/pull/1163)
* browser-like client name (`select distinct http_user_agent from system.query_log`). [#1182](https://github.com/ClickHouse/clickhouse-java/pull/1182)
* enhanced `ClickHouseRequest` by treating input stream and `ClickHouseWriter` equally. [#1200](https://github.com/ClickHouse/clickhouse-java/pull/1200)

### Bug Fixes
* not able to cancel query when there's session_id. [#1035](https://github.com/ClickHouse/clickhouse-java/pull/1035)
* overflow error when handling BigInteger and BigDecimal in ClickHouseLongValue. [#1040](https://github.com/ClickHouse/clickhouse-java/pull/1040)
* not handling SimpleAggregateFunction properly. [#1054](https://github.com/ClickHouse/clickhouse-java/pull/1054)
* DELETE FROM was rewritten to ALTER TABLE DELETE even when lightweight deletion was enabled. [#1063](https://github.com/ClickHouse/clickhouse-java/pull/1063)
* forced headless format for inserting. [#1073](https://github.com/ClickHouse/clickhouse-java/pull/1073)
* missing the verb "be" in error messages. [#1137](https://github.com/ClickHouse/clickhouse-java/pull/1137)
* write time field data loss precision. [#1127](https://github.com/ClickHouse/clickhouse-java/pull/1127)
* fixed a few copy-paste error causing problem handling Geo types
