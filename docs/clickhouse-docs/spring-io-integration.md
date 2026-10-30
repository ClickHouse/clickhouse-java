---
sidebarTitle: 'Spring integration'
keywords: ['java', 'jdbc', 'spring', 'spring boot', 'JdbcTemplate', 'RowSet', 'ZonedDateTime']
description: 'Integrating Spring applications with ClickHouse'
slug: /integrations/language-clients/java/spring-jdbc-template
title: 'Integrating Spring with ClickHouse'
doc_type: 'guide'
integration:
  - support_level: 'core'
  - category: 'language_client'
---

This guide covers different aspects of integrating Spring applications with ClickHouse, including configuration,
data-access APIs, type conversion, and limitations of the Spring abstractions involved.

## `JdbcTemplate` {#jdbc-template}

`JdbcTemplate` is Spring's abstraction for executing JDBC operations and managing their resources. This section focuses
on its read APIs: callbacks, row mappers, convenience query methods, and row sets. Choosing an API matters because each
one hides different logic for iterating, mapping, buffering, and returning data from the driver's `ResultSet`.

### Ways to read data {#jdbc-template-reading-data}

`JdbcTemplate` provides several fetching APIs over the same JDBC driver result:

| API | Interaction with the driver result | Specific behavior or limitation |
| --- | --- | --- |
| `RowCallbackHandler` | Receives the live `ResultSet` at the current row | Spring advances the cursor; the callback does not return a value |
| `ResultSetExtractor` | Receives the complete live `ResultSet` | The extractor controls cursor iteration and constructs the return value |
| `RowMapper` | Receives the live `ResultSet` once per row | Spring collects every mapped object into the returned `List` |
| `queryForObject`, `queryForList`, `queryForMap` | Use Spring's built-in result mapping | The application does not choose which JDBC getters perform the conversion |
| `queryForRowSet` (`SqlRowSet`) | Copies values from the driver result into a `CachedRowSet` | Reads are disconnected from the driver and use the cached Java objects |

Prefer these `query` overloads because they operate on the driver's live `ResultSet` without first copying it into a row
set:

- `RowCallbackHandler` for efficient, independent row-by-row processing.
- `ResultSetExtractor` for full control over iteration and result construction.
- `RowMapper` when returning a list is intentional. Direct access to the JDBC result makes type conversion more
  predictable and portable than relying on a particular `CachedRowSet` implementation.

<Warning>
**Timestamp handling with `queryForRowSet()`**

`queryForRowSet()` is different from the other `queryFor*` methods: it reads the entire driver `ResultSet` into a
disconnected `CachedRowSet` before returning. The `SqlRowSet` returned by Spring does not support
`getObject(column, Class)`, so a call such as `getObject("event_time", ZonedDateTime.class)` fails with
`Not supported yet`.

For `DateTime` and `DateTime64`, configure
`jdbc_type_mappings=DateTime64=java.time.ZonedDateTime,DateTime=java.time.ZonedDateTime` in the connection pool's
`data-source-properties`. The cached value can then be read with the untyped
`(ZonedDateTime) rowSet.getObject("event_time")` call. This mapping applies to every `getObject(column)` call on the
connection, not only to row sets. See the
[Date/Time values guide](/integrations/language-clients/java/jdbc_date_time_guide) for details.
</Warning>

The following example uses the recommended callback API. `JdbcTemplate` advances the cursor and invokes the callback
once for each row; the callback should not call `next()`:

```java
public void processSignals(JdbcTemplate jdbcTemplate, long locationId) {
    String sql = "SELECT device_id, value, event_time "
            + "FROM iot_signals "
            + "WHERE location_id = ? "
            + "ORDER BY event_time";

    jdbcTemplate.query(
        sql,
        (ResultSet rs) -> {
            String deviceId = rs.getString("device_id");
            double value = rs.getDouble("value");
            ZonedDateTime eventTime = rs.getObject("event_time", ZonedDateTime.class);
            // process the row
        },
        locationId);
}
```

For the complete API and callback contracts, see the
[Spring JDBC documentation](https://docs.spring.io/spring-framework/reference/data-access/jdbc/core.html).
