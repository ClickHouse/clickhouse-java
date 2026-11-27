---
name: migrate-v1-client-properties
description: >-
  Converts V1 ClickHouse client configuration keys, option maps, and URL query
  parameters to V2 using migration-helpers ConfigurationMigrationHelper. Use
  when migrating ClickHouseClientOption, ClickHouseConfig, ClickHouseHttpOption,
  connection properties, or client URL parameters from com.clickhouse.client to
  client-v2. Leaves a comment for every excluded property.
---

# Migrate V1 client properties

Convert V1 configuration into V2 keys. Rewrite the user's code. Do not edit the repo examples unless the user asked to change those files. Do not turn a JDBC URL into a `Client`. Query parameters on a client or JDBC URL still follow `convertUrl`.

Construction, reads, and writes belong to `migrate-v1-client-instantiation`, `migrate-v1-client-reads`, and `migrate-v1-client-writes`.

## Read the helper first

The files are the authority. Read them before converting. If they disagree with [reference.md](reference.md), follow the files.

- `migration-helpers/src/main/java/com/clickhouse/migration/config/ConfigurationMigrationHelper.java` (`convertMap`, `convertProperties`, `convertUrl`)
- `migration-helpers/src/main/resources/com/clickhouse/migration/config/v1-to-v2-mappings.properties`
- `migration-helpers/src/main/resources/com/clickhouse/migration/config/v1-deprecated-properties.properties`
- `migration-helpers/src/main/resources/com/clickhouse/migration/config/v2-known-properties.properties`
- `migration-helpers/migrating_properties.md`

Walk every key yourself. The helper debug-logs drops and does not return exclusion reasons, so running it is not a substitute for the walk. Do not invent renames.

Apply `convertMap` in this order. Keys are trimmed. Prefix and special-case checks are case-insensitive.

1. Key already starts with `clickhouse_setting_` or `http_header_`: keep it. Normalize the value.
2. `custom_settings`, `custom_http_params`, or `custom_params`: split into `clickhouse_setting_<name>` pairs.
3. `custom_http_headers` or `custom_headers`: split into `http_header_<name>` pairs.
4. `http_keep_alive`: `false` or `0` becomes `http_keep_alive_timeout=0`. Any other value is omitted. This runs even though the key is in the deprecated file.
5. Key in `v1-to-v2-mappings.properties` whose target differs from the source: rename. A mapping that only echoes the same name is not a rename.
6. Key in `v1-deprecated-properties.properties`: drop it.
7. Known v2 property: listed in `v2-known-properties.properties`, or a `getKey()` value on `ClientConfigProperties` or `com.clickhouse.jdbc.DriverProperties`. `ConfigPropertyCache` loads those enums in addition to the file. Keep the canonical key.
8. Anything else: prefix `clickhouse_setting_`. V1 treated unknown keys as server settings. The helper does this for `max_threads` (`clickhouse_setting_max_threads`). `migrating_properties.md` also calls the old client thread-pool `max_threads` deprecated, so comment that the prefixed key is a server setting and should be removed if the original value was only the thread pool.

`normalizePropertyValue`: when the stored key is `ssl_mode` or `sslmode` and the value is `none`, write `TRUST`.

Null values are omitted from `Properties` output.

## Comments

Put a comment on every excluded key next to the converted code, not only in a summary. Take the reason from `migrating_properties.md` when that file mentions the key. Otherwise say it is deprecated with no v2 conversion.

When the notes say a **kept** key is deprecated or means something else, keep the helper's result and add a comment. Do not drop it.

- `ssl`: helper keeps it. Comment that `migrating_properties.md` says the old `ssl` flag is ignored, and that encryption comes from an `https://` endpoint.
- `async=true`: keep the value. Comment that V2 defaults to false and that `migrate-v1-client-writes` decides whether to keep a background executor. Do not flip it in this skill.
- `max_execution_time`: the mapping renames it to `clickhouse_setting_max_execution_time`. Comment that the V2 client property of the same name has another meaning for async operations. Do not also set the client property.

JDBC-only keys the helper drops (`fetch_size`, `dialect`, and the rest of the deprecated list) get the same exclusion comment.

## Output

Return:

- the updated properties, builder `setOption` / `httpHeader` / `serverSetting` calls, or URL
- a comment for each excluded key
- a short list of renamed keys and excluded keys

Prefer `httpHeader` and `serverSetting` when the surrounding code is a `Client.Builder`. Keep a `Properties` object when the source was a property bag. Use a typed builder method only when `Client.Builder` already has one for that V2 key.
