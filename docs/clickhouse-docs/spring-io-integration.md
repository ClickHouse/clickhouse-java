---
sidebarTitle: 'Spring JdbcTemplate'
keywords: ['java', 'jdbc', 'spring', 'spring boot', 'JdbcTemplate', 'RowSet', 'ZonedDateTime']
description: 'Using the ClickHouse JDBC driver with Spring JdbcTemplate'
slug: /integrations/language-clients/java/spring-jdbc-template
title: 'Using the ClickHouse JDBC driver with Spring JdbcTemplate'
doc_type: 'guide'
integration:
  - support_level: 'core'
  - category: 'language_client'
---

Spring's `JdbcTemplate` works with the ClickHouse JDBC driver like with any other JDBC driver. However, ClickHouse queries
often return large result sets, so the way the result is read matters. This guide describes which `JdbcTemplate` APIs to
prefer, which to avoid for large results, and how to read `DateTime`/`DateTime64` values.

## Configuration {#configuration}

With Spring Boot, the driver is configured through the regular `spring.datasource` properties. Driver-specific properties
are passed with `data-source-properties` of the connection pool:

```yaml
spring:
  datasource:
    url: jdbc:clickhouse://localhost:8123/default
    username: default
    password:
    driver-class-name: com.clickhouse.jdbc.ClickHouseDriver
    hikari:
      data-source-properties:
        # driver properties go here, for example:
        jdbc_type_mappings: DateTime64=java.time.ZonedDateTime,DateTime=java.time.ZonedDateTime
```

## Reading results {#reading-results}

`JdbcTemplate` has three groups of read APIs. They differ in how much of the result is kept in memory.

| API | Example | Memory |
| --- | --- | --- |
| Callback (`RowCallbackHandler`, `ResultSetExtractor`) | `query(sql, handler, args...)` | One row at a time. The driver streams the result. |
| List (`RowMapper`) | `query(sql, rowMapper, args...)`, `queryForList(...)` | All mapped objects. |
| Row set (`SqlRowSet`) | `queryForRowSet(sql, args...)` | All rows, copied into a `CachedRowSet`. |

### Callback API (recommended for large results) {#callback-api}

A `RowCallbackHandler` is called for every row while the driver reads the response from the server. Nothing is
accumulated, so memory usage does not depend on the number of rows. This is the preferred way to export, aggregate or
forward large result sets:

```java
jdbcTemplate.query("""
        SELECT device_id, value, event_time
        FROM iot_signals
        WHERE location_id = ?
        ORDER BY event_time
        """,
        (ResultSet rs) -> {
            String deviceId = rs.getString("device_id");
            double value = rs.getDouble("value");
            ZonedDateTime eventTime = rs.getObject("event_time", ZonedDateTime.class);
            // process the row
        },
        locationId);
```

Use `ResultSetExtractor` when the whole result should be reduced to a single value (for example, a summary object), and
the `ResultSet` should be iterated manually.

### List API (recommended for small and medium results) {#list-api}

A `RowMapper` maps each row to an object and `JdbcTemplate` returns a `List` of them. It is the most convenient API, and
is fine when the result fits in memory comfortably (for example, a page of rows or an aggregated report):

```java
record Signal(String deviceId, double value, ZonedDateTime eventTime) {}

List<Signal> signals = jdbcTemplate.query(
        "SELECT device_id, value, event_time FROM iot_signals WHERE location_id = ? LIMIT 1000",
        (rs, rowNum) -> new Signal(
                rs.getString("device_id"),
                rs.getDouble("value"),
                rs.getObject("event_time", ZonedDateTime.class)),
        locationId);
```

Use `LIMIT` or aggregation on the server side to keep the list size bounded.

### Row set API {#row-set-api}

`queryForRowSet()` returns a `SqlRowSet` that is backed by a `javax.sql.rowset.CachedRowSet`. The row set reads the
**whole** result into memory before the first row is returned, and every value is stored as a Java object. For large
results this means high memory usage and possible `OutOfMemoryError`. Prefer the [callback API](#callback-api) for such
cases.

The row set API is still useful when a disconnected, scrollable result is needed (for example, to pass small results
between layers). In this case, note how timestamps are cached.

#### Reading `DateTime` values from a row set {#row-set-date-time}

`CachedRowSet` fills itself by calling `ResultSet#getObject(int)` for every column, so it stores whatever class the driver
maps the column to by default. For `DateTime` and `DateTime64` it is `java.sql.Timestamp`. Additionally, the JDK
`CachedRowSet` implementation doesn't support `getObject(column, Class)`: calling `getObject("event_time", ZonedDateTime.class)`
on a `CachedRowSet` (or on `SqlRowSet`) fails with `Not supported yet`.

To get `java.time.ZonedDateTime` values from a row set, configure the driver to map timestamp types to `ZonedDateTime`
with the `jdbc_type_mappings` property:

```yaml
spring:
  datasource:
    hikari:
      data-source-properties:
        jdbc_type_mappings: DateTime64=java.time.ZonedDateTime,DateTime=java.time.ZonedDateTime
```

Then the row set caches `ZonedDateTime` objects that can be read with `getObject(column)`:

```java
SqlRowSet rows = jdbcTemplate.queryForRowSet(
        "SELECT device_id, event_time FROM iot_signals WHERE location_id = ? LIMIT 100", locationId);
while (rows.next()) {
    ZonedDateTime eventTime = (ZonedDateTime) rows.getObject("event_time");
    // ...
}
```

`ZonedDateTime` is more portable and convenient than `Timestamp`: it keeps the time zone of the value, it can be
converted to any other zone or to `Instant`/`LocalDateTime` explicitly, and its `toString()` doesn't depend on the JVM
default time zone.

<Note>
`jdbc_type_mappings` affects every `getObject(column)` call on the connection, not only row sets.
With this mapping, `getTimestamp()` on a row set fails with `ClassCastException` because the cached value is a
`ZonedDateTime`. Read such columns with `getObject(column)` only.
</Note>

## Avoid `java.sql.Timestamp` {#avoid-timestamp}

<Warning>
`java.sql.Timestamp` is a complicated class. It extends `java.util.Date` but stores nanoseconds separately, it has no
time zone while its `toString()` and `valueOf()` use the JVM default time zone, and its `equals()` is not symmetric
with `java.util.Date`. The same ClickHouse value can look different depending on the JVM time zone, which is a common
source of bugs.

Prefer `java.time.ZonedDateTime` (`rs.getObject(column, ZonedDateTime.class)`), or `java.time.LocalDateTime`
when the time zone is known to the application.
See the [Date/Time values guide](/integrations/language-clients/java/jdbc_date_time_guide) for how the driver
converts `DateTime`/`DateTime64` values and how time zones affect them.
</Warning>
