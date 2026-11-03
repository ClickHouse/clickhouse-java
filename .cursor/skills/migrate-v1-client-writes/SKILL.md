---
name: migrate-v1-client-writes
description: >-
  Rewrites V1 ClickHouse inserts (write, ClickHouseRequest.Mutation, data,
  ClickHousePipedOutputStream, load) to client-v2 insert and InsertSettings.
  Use when migrating V1 insert or write code from com.clickhouse.client to
  client-v2, including mutation server settings versus client options, or when
  deciding whether to keep async=true. V2 is synchronous by default.
---

# Migrate V1 client writes

Rewrite `com.clickhouse.client` inserts to `com.clickhouse.client.api.Client.insert`. Rewrite the user's code. Do not edit the repo examples unless the user asked to change those files.

Leave construction to `migrate-v1-client-instantiation`, reads to `migrate-v1-client-reads`, and property bags to `migrate-v1-client-properties`.

V1 builds a `ClickHouseRequest` and then a `ClickHouseRequest.Mutation` (`read(...).write()` or `client.write(...)`). The mutation copies the request settings map and still accepts `set`, `option`, `settings`, and `session`. Table, input format, and the data writer sit on that same object. V2 passes the table, the payload, and the format as `insert` arguments. Server settings, headers, query id, and session go on `InsertSettings`.

## Review the mutation first

Before rewriting, walk the request and the mutation and build a map. Show the map when `set` and `option` are both used.

`set` is a server setting. `option` is a client option. The same exceptions as on a read request apply, because `Mutation` extends `ClickHouseRequest`:

- `set("query_id" | "session_id" | "session_check" | "session_timeout", ...)` is redirected to `option`, not stored as a server setting.
- `set(String, String)` quotes the value. Pass the original text to `serverSetting`.
- `option(CUSTOM_SETTINGS, "a=1,b=2")` unpacks into server settings.
- `option(MAX_EXECUTION_TIME)`, `MAX_RESULT_ROWS`, and `RESULT_OVERFLOW_MODE` are client options that the HTTP layer also sends as server settings when the settings map lacks that key.
- `session(...)` is stored as client options and then sent as session server settings.
- Settings copied from `custom_settings` at request construction, then copied again onto the mutation, are server settings even with no `set` at the call site.
- `params(...)` are SQL parameters, not settings.
- `CUSTOM_PARAMS` are raw query-string pairs. Use `serverSetting` only when the name is a ClickHouse setting.
- `CUSTOM_HEADERS` are headers.

| V1 on the request or mutation | V2 |
|---|---|
| `table(name)` | first argument of `insert` |
| `format(...)` | `ClickHouseFormat` argument. `Mutation.format` also appends `FORMAT` to the SQL. Do not hand-build that clause |
| `data(writer)`, piped stream, `data(InputStream)`, `load(...)` | `DataStreamWriter`, `InputStream`, or POJO list argument |
| `set` / `settings` / unpacked `custom_settings` | `InsertSettings.serverSetting` |
| `set("query_id", id)` or `table(name, queryId)` | `settings.setQueryId(id)` |
| `session(...)` | `settings.use(session)` |
| client-only `option` (`ASYNC`, compression) | properties skill, or `InsertSettings.setOption` when it is per insert |
| custom headers | `settings.httpHeader` |
| SQL `insert` with no body | `execute(sql, new CommandSettings())`, not `insert` |

`max_execution_time` on the mutation becomes `serverSetting("max_execution_time", ...)`. Do not set the V2 client property of that name. It means a different async-operation timeout.

## Say this, then ask

V2 runs each operation on the calling thread (`async` defaults to false). `DataStreamWriter.onOutput` runs on that same thread, once per attempt, and the client closes the stream. A V1 piped insert (`ClickHousePipedOutputStream` plus `execute()` on a worker, as in `examples/client/.../jdbc/Main.java`) does not need a second thread.

Ask whether the user still wants the operation scheduled on another executor via `useAsyncRequests(true)`. Use the AskQuestion tool when it is available:

- No, keep it synchronous
- Yes, call `useAsyncRequests(true)`

Skip the question only when the conversation already answered it. If you cannot ask, keep it synchronous and say that assumption.

Call `useAsyncRequests(true)` only when the user says yes. Do not preserve V1 `async=true` just because it was the old default. With the default, `.get()` runs the work on the caller. Do not wrap that in another executor.

When they opt in:

```java
new Client.Builder()
        .useAsyncRequests(true)
        .setSharedOperationExecutor(executor)
        .build();
```

`Client.close()` does not shut that executor down.

## Choose the insert

| V1 | V2 |
|---|---|
| Piped `RowBinary`, `BinaryStreamUtils`, or a `ClickHouseWriter` lambda | `insert(table, out -> {...}, format, settings)` and `RowBinaryFormatWriter` inside the callback |
| `load` or `data(InputStream)` of JSON, CSV, or another format | `insert(table, stream, format, settings)` |
| POJO list | `register(Class, client.getTableSchema(table))` then `insert(table, list, settings)` |
| `getSummary().getWrittenRows()` | `InsertResponse.getWrittenRows()` |
| SQL insert with no body | `execute(sql, commandSettings)` |
| Protobuf or other raw bytes | `insert(..., out -> {...}, that format, settings)` without `RowBinaryFormatWriter` |

## Examples

Piped RowBinary. V1 starts `execute()` on a worker and writes the pipe on the caller. V2 writes inside the callback on the caller thread. `setValue` indexes are 1-based. `setValue(column, null)` covers a null. Do not copy `BinaryStreamUtils` byte layout when this writer is used.

```java
ClickHouseRequest.Mutation request = client.read(server).write().table(table)
        .format(ClickHouseFormat.RowBinary)
        .set("max_threads", 2);
try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
        .createPipedOutputStream(request.getConfig(), (Runnable) null)) {
    CompletableFuture<ClickHouseResponse> future = request.data(stream.getInputStream()).execute();
    BinaryStreamUtils.writeString(stream, "a");
    // ... write the other columns, then close stream and future.get()
}
```

```java
InsertSettings settings = new InsertSettings().serverSetting("max_threads", "2");
TableSchema schema = client.getTableSchema(table);
ClickHouseFormat format = ClickHouseFormat.RowBinary;
try (InsertResponse response = client.insert(table, out -> {
    RowBinaryFormatWriter writer = new RowBinaryFormatWriter(out, schema, format);
    writer.setValue(1, "a");
    writer.setValue(2, "b");
    writer.commitRow();
}, format, settings).get()) {
    long written = response.getWrittenRows();
}
```

`RowBinaryFormatWriter` is `com.clickhouse.client.api.data_formats.RowBinaryFormatWriter`. `InsertResponse` is `com.clickhouse.client.api.insert.InsertResponse`. `getWrittenRows()` is the `NUM_ROWS_WRITTEN` server metric. Use `RowBinaryWithDefaults` only when the V1 payload omitted columns that have defaults. Column subset: `insert(table, columnNames, writer, format, settings)`.

Session and query id on the mutation use the same settings object. They are not server settings from `set`.

```java
Session session = new Session().setSessionId("abc").setSessionCheck(true);
InsertSettings settings = new InsertSettings()
        .setQueryId("ins-1")
        .serverSetting("insert_quorum", "2")
        .use(session);
```

JSON or CSV stream (`Stream2DbWriter`):

```java
try (InsertResponse response = client.insert(table, inputStream,
        ClickHouseFormat.JSONEachRow, settings).get()) {
    response.getWrittenRows();
}
```

POJO list. `register` once (`POJO2DbWriter`):

```java
client.register(ArticleViewEvent.class, client.getTableSchema(table));
client.insert(table, events, settings).get();
```

Protobuf and other raw formats stay raw bytes in `onOutput`. Pass `ClickHouseFormat.Protobuf`. Do not wrap them in `RowBinaryFormatWriter`.

An `insert into t values (...)` with no data stream is `client.execute(sql, new CommandSettings()).get()`.

## Callback rules

- Write every row inside `onOutput`. Do not start the insert and write a pipe from the caller.
- A callback that generates rows can run again on retry. For a one-shot `InputStream` that cannot `reset()`, say so: the stream overload calls `reset()` from `onRetry`.
- Close `InsertResponse`.
- Client-wide `custom_settings` that apply to every insert can move to `Client.Builder.serverSetting`. A `set` on the mutation overrides that key for this call.
- Compression and retry details are in [reference.md](reference.md).

## Output

Return the classification map, whether the insert stays synchronous, the rewritten insert, and one line naming the V1 write style that was matched.
