---
name: migrate-v1-client-reads
description: >-
  Rewrites V1 ClickHouse client reads (client.read, ClickHouseRequest, query,
  execute, ClickHouseResponse, records, stream) to the closest client-v2 query,
  queryRecords, queryAll, or execute call. Use when migrating V1 query or read
  code from com.clickhouse.client to client-v2, including request.set server
  settings versus client options.
---

# Migrate V1 client reads

Rewrite `com.clickhouse.client` reads to `com.clickhouse.client.api.Client`. Rewrite the user's code. Do not edit the repo examples unless the user asked to change those files.

Leave client construction to `migrate-v1-client-instantiation` and property bags to `migrate-v1-client-properties`. Inserts belong to `migrate-v1-client-writes`.

V1 puts SQL, format, server settings, client options, and parameters on one `ClickHouseRequest` from `client.read(...)`. V2 passes SQL and query parameters as arguments. Format, server settings, headers, query id, and session go on a `QuerySettings` (or `CommandSettings` for `execute`).

## Review the V1 request first

Before rewriting, walk every call on that request and build a map. Show the map when `set` and `option` are both used. Do not treat them as the same thing.

`ClickHouseRequest.set` is documented as a **server setting** (it becomes an HTTP query parameter). `option` is a **client option**. These calls break that rule:

- `set("query_id" | "session_id" | "session_check" | "session_timeout", ...)` does not store a server setting. Those names are redirected to `option`.
- `set(String, String)` quotes the value (`ClickHouseUtils.escape`). Pass the original text to `serverSetting`, not the quoted form.
- `option(CUSTOM_SETTINGS, "a=1,b=2")` is unpacked into `set`, so each pair is a server setting.
- `option(MAX_EXECUTION_TIME)`, `option(MAX_RESULT_ROWS)`, and `option(RESULT_OVERFLOW_MODE)` are client options that the HTTP layer also sends as server settings when the request settings map does not already contain that key.
- `session(...)` stores `session_id`, `session_check`, and `session_timeout` as client options. The HTTP layer then sends them as server settings.
- The request is constructed with `client.getConfig().getCustomSettings()` already copied into its settings map. Those keys are server settings even when the call site never calls `set`.
- `params(...)` are SQL parameters, not server settings and not client options.
- `ClickHouseHttpOption.CUSTOM_PARAMS` are raw query-string pairs. Map a pair to `serverSetting` only when the name is a ClickHouse setting. Otherwise leave a comment.
- `CUSTOM_HEADERS` are headers, not settings.

| V1 on the request | V2 |
|---|---|
| `query(sql)` | SQL argument of `query`, `queryRecords`, `queryAll`, or `execute` |
| `query(sql, queryId)` or `set("query_id", id)` | `settings.setQueryId(id)` |
| `format(...)` | `QuerySettings.setFormat`. Do not add a `FORMAT` clause |
| `set(name, value)`, `settings(map)`, unpacked `custom_settings` | `settings.serverSetting(name, value)` |
| `session(...)` or the session names above | `settings.use(session)` |
| `option` that is only client behavior (`ASYNC`, compression, timeouts) | properties skill, or `settings.setOption` when it is per query |
| `params(...)` | `Map<String, Object>` argument. `:name` / `:name(Type)` becomes `{name:Type}` |
| `http` custom headers | `settings.httpHeader` |

`max_execution_time` as a server setting becomes `serverSetting("max_execution_time", ...)`. The V2 client property of the same name is a different async-operation timeout. Do not set that client property here.

## Choose the call

| What the V1 code consumes | V2 call |
|---|---|
| `response.records()` or `stream()` of `ClickHouseRecord` | `queryRecords(sql, settings).get()` |
| Every row already copied into a `List` | `queryAll(sql, settings)` |
| `getInputStream()`, `pipe`, or a text/Protobuf body | `query(sql, settings).get()` then `getInputStream()` |
| `records(Pojo.class)` | `register` plus experimental `queryAll(sql, clazz, schema, allocator)` |
| DDL or a statement whose body is ignored | `execute(sql, new CommandSettings())` |

`queryRecords` forces `RowBinaryWithNamesAndTypes`. If the request format is not a RowBinary format, do not use `queryRecords`. Put that format on `QuerySettings` and read `getInputStream()`, or `newBinaryFormatReader` for `Native` and `RowBinary*`.

Use `queryAll` only when the V1 code already materializes the full result.

## Examples

Records. V1 `getValue(0)` is 0-based. V2 `getString(1)` is 1-based. `asString` / `asInteger` / `asLong` become `getString` / `getInteger` / `getLong`.

```java
try (ClickHouseResponse response = client.read(server)
        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
        .query("select a, b from " + table)
        .execute().get()) {
    for (ClickHouseRecord r : response.records()) {
        r.getValue(0).asString();
        r.getValue("b").asString();
    }
}
```

```java
try (Records records = client.queryRecords("select a, b from " + table).get()) {
    for (GenericRecord record : records) {
        record.getString(1);
        record.getString("b");
    }
}
```

Server settings mixed with client options. Classify first, then build `QuerySettings`.

```java
client.read(server)
        .query("select a from t", "q-1")
        .set("max_threads", 2)                         // server setting
        .set("log_comment", "load")                    // server setting, value is quoted by this overload
        .session("abc", true, 60)                      // client options later sent as session settings
        .option(ClickHouseClientOption.MAX_EXECUTION_TIME, 30) // also sent as a server setting
        .option(ClickHouseClientOption.ASYNC, false)   // client option only
        .format(ClickHouseFormat.JSONEachRow)
        .execute().get();
```

```java
Session session = new Session()
        .setSessionId("abc")
        .setSessionCheck(true)
        .setSessionTimeout(60);
QuerySettings settings = new QuerySettings()
        .setQueryId("q-1")
        .setFormat(ClickHouseFormat.JSONEachRow)
        .serverSetting("max_threads", "2")
        .serverSetting("log_comment", "load")
        .serverSetting("max_execution_time", "30")
        .use(session);
// ASYNC false is the V2 default. Do not call useAsyncRequests.
try (QueryResponse response = client.query("select a from t", settings).get()) {
    InputStream body = response.getInputStream();
}
```

SQL parameters stay off the settings object:

```java
Map<String, Object> params = new HashMap<>();
params.put("id", 1);
client.query("select * from t where id = {id:UInt32}", params, settings).get();
```

Comment any `{name:Type}` type that V1 wrote as `:name` with no type.

Whole list, only when V1 already collected every row:

```java
List<GenericRecord> rows = client.queryAll(sql, settings);
```

POJO list, after `client.register(Pojo.class, schema)`. Experimental:

```java
List<Pojo> rows = client.queryAll(sql, Pojo.class, schema, Pojo::new);
```

Binary rows when the caller uses column readers rather than `records()`:

```java
try (QueryResponse response = client.query(sql, settings).get()) {
    ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
    while (reader.hasNext()) {
        reader.next();
        reader.getString("title");
    }
}
```

DDL that ignores the body. `CommandSettings` extends `QuerySettings`, so server settings use the same methods.

```java
CommandSettings settings = new CommandSettings();
settings.serverSetting("max_execution_time", "30");
client.execute("drop table if exists t", settings).get();
```

## Rewrite rules

- `execute().get()` and `executeAndWait()` become `.get()`. The future is completed on the caller thread unless `useAsyncRequests(true)` was requested. Do not add that flag here.
- Close `QueryResponse` and `Records`.
- Client-wide `custom_settings` that apply to every read can move to `Client.Builder.serverSetting`. A later `set` on the request overrides that key for this call.

## No silent substitute

Leave a comment, and do not invent an API, for `external(...)`, transactions, and `output(OutputStream)` dumps. Name the missing V1 call. Copying `getInputStream()` onto an `OutputStream` is the substitute for `pipe` / `output`. `firstRecord()` is the first `GenericRecord` or one `reader.next()`. Multi-node `read(nodes)` belongs to the instantiation skill. The same list is in [reference.md](reference.md).

## Output

Return the classification map, the rewritten read, and one line naming which V1 consumption mapped to which V2 method.
