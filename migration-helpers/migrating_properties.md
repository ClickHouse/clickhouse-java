# Migration Notes for ugprade from 0.6.x to 0.9.x

## Host specification

- No multihost allowed in jdbc URL like `jdbc:ch://host1,host2:8123/`.
- `ssl_mode` was redefined in `0.10.0`. In `0.9.x` it is handled by JDBC and can be only `STRICT`
- HTTP protocol is not guessed by port anymore. Default is plain HTTP. Otherwise should be like `jdbc:ch:https://cloud.com:8443/

## Authentication

- Plain user-password is unchanged.
- New SSL modes for self-signed certificates added in `0.10.0`.
- `http_use_basic_auth` (default: `true`) to send authentication credentials. was introduced in V2 and backported to V1


## Protocol Configuration

- `protocol` - deprecated. Only http supported. Can be ignored in JDBC case because URL defines protocol.

### Connection

- `sslcerttype` - is deprecated. But will be re-introduced soon with a new name. Can be ignored if X.509 requested
- `sslkeyalg` - is deprecated. But will be re-introduced soon with a new name. Can be ignored if RSA requested.
- `sslprotocol` - is deprecated. Currently only latest protocol is available. Can be ignored.
- `custom_socket_factory` - is deprecated.
- `custom_socket_factory_options` - is deprecated.
- `connect_timeout` - is replaced with `connection_request_timeout` and `connection_timeout`. V1 used same timeout for getting
  connection from pool and timeouting establishing new connection.
- `ssl` - is deprecated. Ignored.
- `sslmode` - replaced by `ssl_mode` with more values.

### TCP Socket Configuration

- `socket_ip_tos` - is deprecated

### HTTP Configuration

- `http_connection_provider` - is deprecated and has to be ignored.
- `custom_http_headers` - is deprecated. Custom headers should be set by one and with `http_header_` prefix.
- `custom_http_params` - is deprecated. If custom http parameter is clickhouse setting it should be set with `clickhouse_setting_` prefix.
  There is another case when query parameters have user define meaning. The should be with prefix set in DB configuration like `custom_` (see more https://clickhouse.com/docs/reference/settings/server-settings/settings/custom#custom_settings_prefixes).

- `http_server_default_response` - is deprecated. Can be ignored for JDBC case.
- `receive_query_progress` - is deprecated. Not supported and can be ignored for JDBC case.
- `send_http_client_id` - is deprecated. Can be ignored for JDBC case.
- `wait_end_of_query` - is really a server setting - should be prefixed with `clickhouse_setting_`
- `remember_last_set_roles` - valid for JDBC only. List of roles should be set via `session_db_roles` if working with client directly.
- `ahc_validate_after_inactivity` - is deprecated. Can be ignored. Validation made automatically.
- `ahc_retry_on_failure` - is deprecated. Two new properties `retry` (for number of retries) and `client_retry_on_failures` (to configure when to retry. Possible values: `NoHttpResponse`, `ConnectTimeout`, `ConnectionRequestTimeout`, `ServerRetryable`)

- `alive_timeout` and `http_keep_alive` - are deprecated and joined into `http_keep_alive_timeout`.


## Client Operation Side

- `use_compilation` - is deprecated.
- `debug_measure_request_time` - is deprecated.


### Multithreading
- `async` - this defined if each operation is run in separate thread. V2 switched to `false` by default.
- `max_scheduler_threads` - is deprecated. scheduler is set via configuration and defined by user.
- `max_threads` - is deprecated.
- `max_requests` - is deprecated.
- `thread_keepalive_timeout` - is deprecated.
- `max_core_thread_ttl` - is deprecated.


### Server Endpoints

- `auto_discovery` - is deprecated.
- `load_balancing_policy` - is deprecated. Load balancing is not part of Client main functionality.
- `load_balancing_tags` - is deprecated.
- `health_check_interval` - is deprecated.
- `health_check_method` - is deprecated.
- `node_discovery_interval` - is deprecated.
- `node_discovery_limit` - is deprecated.
- `node_check_interval` - is deprecated.
- `node_group_size` - is deprecated.
- `check_all_nodes` - is deprecated.
- `version` - is replaced by `server_version`.
- `server_revision` - is replaced by `server_version`.
- `failover` - is deprecated.

### Server Interaction

- `custom_settings` - is deprecated. Was used to define client wide list of server settings. Now each settings should be set separatly and
  with `clickhouse_setting_` prefix.
- `time_zone` - is replaced by `server_time_zone`
- `auto_session` - is deprecated. Sessions are created using client API. JDBC has no direct control over it.
- `log_leading_comment` - is deprecated and was applicable for JDBC. When true JDBC was parsing leading comment and sent to
  server via `log_comment`.
- `max_execution_time` - Should be replaced with server setting (`clickhouse_setting_max_execution_time`). However V2
  client has similar setting with another meaning for async operations.
- `max_result_rows` - Should be replaced with server setting (`clickhouse_setting_max_result_rows`).
- `result_overflow_mode` - Should be replaced with server setting (`clickhouse_setting_result_overflow_mode`).
- `product_name` - replaced by `client_name`.
- `rename_response_column` - is deprecated.
- `transaction_timeout` - is deprecated.

### Sessions
- `repeat_on_session_lock` - is deprecated. But need to be implemented as part of retry logic.
- `session_id` - Should be replaced with server setting (`clickhouse_setting_session_id`).
- `session_check` - Should be replaced with server setting (`clickhouse_setting_session_check`).
- `session_timeout` - Should be replaced with server setting (`clickhouse_setting_session_timeout`).


### Data Transfer

- `buffering` - is deprecated.
- `buffer_size` - is deprecated.
- `buffer_queue_variation` - is deprecated.
- `use_blocking_queue` - is deprecated.
- `read_buffer_size` - is deprecated.
- `write_buffer_size` - is deprecated.
- `request_chunk_size` - is deprecated.
- `request_buffering` - is deprecated.
- `response_buffering`- is deprecated.
- `compress_algorithm` - is deprecated.
- `decompress_algorithm` - is deprecated.
- `compress_level` - is deprecated.
- `decompress_level` - is deprecated.
- `max_buffer_size` - is deprecated.
- `max_mapper_cache` - is deprecated.
- `max_queued_buffers` - is deprecated.
- `max_queued_requests` - is deprecated.
- `rounding_mode` - is deprecated.
- `srv_resolve` - is deprecated.
- `reuse_value_wrapper` - is deprecated.
- `widen_unsigned_types` - is deprecated.
- `use_binary_string` - replaced with `binary_string_support`. applicable only for `0.10.0`
- `use_objects_in_arrays` - is deprecated.
- `use_server_time_zone_for_dates` - is deprecated.
