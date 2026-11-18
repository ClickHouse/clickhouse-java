# Property conversion reference

Authority is `ConfigurationMigrationHelper` and the resource files named in `SKILL.md`. This file is the comment and output shape. Re-read the property files before converting; the snapshot below can go stale.

## Current renames (`v1-to-v2-mappings.properties`)

| V1 | V2 |
|---|---|
| `connect_timeout` | `connection_timeout` |
| `buffer_size` | `client_network_buffer_size` |
| `sslmode` | `ssl_mode` |
| `sslkey` | `ssl_key` |
| `proxy_username` | `proxy_user` |
| `alive_timeout` | `http_keep_alive_timeout` |
| `version` | `server_version` |
| `server_revision` | `server_version` |
| `time_zone` | `server_time_zone` |
| `product_name` | `client_name` |
| `use_binary_string` | `binary_string_support` |
| `typeMappings` | `jdbc_type_mappings` |
| `databaseTerm` | `jdbc_schema_term` |
| `max_execution_time` | `clickhouse_setting_max_execution_time` |

`sslmode=none` becomes `ssl_mode=TRUST`.

## Special cases the deprecated file does not describe

| V1 | Result |
|---|---|
| `custom_settings`, `custom_http_params`, `custom_params` | Each pair becomes `clickhouse_setting_<name>` |
| `custom_http_headers`, `custom_headers` | Each pair becomes `http_header_<name>` |
| `http_keep_alive=false` or `0` | `http_keep_alive_timeout=0` |
| `http_keep_alive` with any other value | Omitted. V2 keeps its default keep-alive. |
| Unknown key `foo` | `clickhouse_setting_foo` |
| `max_threads` | `clickhouse_setting_max_threads` (helper tests). Comment that the notes deprecate the old client thread-pool setting of the same name. |
| Already `clickhouse_setting_*` or `http_header_*` | Unchanged |

Pair parsing uses `ClientConfigProperties.toKeyValuePairs`. Do not invent a different splitter.

## Keys the notes discuss and the helper keeps

| Key | What to write |
|---|---|
| `ssl` | Keep. Comment: migrating notes say this flag is ignored; use an `https://` endpoint. |
| `async` | Keep. If the value is `true`, comment: V2 defaults this to false; `migrate-v1-client-writes` asks before `useAsyncRequests(true)`. |
| `max_execution_time` | Rename via the mapping. Comment: the client property `max_execution_time` is a different async-operation timeout. |

## Exclusion comment

```java
Client client = new Client.Builder()
        .addEndpoint("http://localhost:8123")
        .setOption("connection_timeout", "5000") // renamed from connect_timeout
        .serverSetting("max_threads", "8")       // helper prefixes this; notes call the old thread-pool max_threads deprecated — remove if it was not a server setting
        // excluded: failover — deprecated, load balancing is not part of Client
        // excluded: custom_socket_factory — deprecated, no v2 conversion
        // excluded: http_keep_alive=true — omitted; v2 keep-alive stays at its default
        .build();
```

A property bag uses the same comments above each dropped `setProperty`.

## URL

`convertUrl` rewrites only the query string. `jdbc:clickhouse://localhost:8123/db?connect_timeout=5000` becomes `connection_timeout=5000` and keeps the JDBC scheme. Do not emit a `Client` from that URL.

`migrating_properties.md` says a multi-host URL such as `jdbc:ch://host1,host2:8123/` is not supported. `convertUrl` does not rewrite the host list. Comment that the host list was left unchanged.

## Summary shape

After the code, list:

- Renamed: `connect_timeout` -> `connection_timeout`
- Excluded: `failover` — deprecated, no v2 conversion
- Kept with a note: `ssl`, `async=true`
