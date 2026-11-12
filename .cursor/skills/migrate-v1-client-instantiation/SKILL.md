---
name: migrate-v1-client-instantiation
description: >-
  Rewrites V1 ClickHouseClient and ClickHouseNode construction into a client-v2
  Client.Builder. Use when migrating clickhouse-java client instantiation,
  ClickHouseClient.newInstance, ClickHouseClient.builder, ClickHouseNode,
  ClickHouseNodes, ClickHouseCredentials, or com.clickhouse.client setup to
  com.clickhouse.client.api.Client.
---

# Migrate V1 client instantiation

Rewrite `com.clickhouse.client` construction into one `com.clickhouse.client.api.Client`. Rewrite the user's code. Do not edit the repo examples unless the user asked to change those files. Do not turn JDBC or R2DBC into a `Client`.

Leave option maps and `ClickHouseConfig` to `migrate-v1-client-properties`. Leave query and insert call sites to `migrate-v1-client-reads` and `migrate-v1-client-writes`.

Read [reference.md](reference.md) before rewriting. Confirm builder methods against `client-v2` `Client.Builder` if a method name is not in that file.

## Ask first

Ask the questions below before rewriting. Skip a question the pasted code or the conversation already answers, and state that assumption. Use the AskQuestion tool when it is available.

1. How many servers does this client connect to?
   - One endpoint
   - Several endpoints and the application picks which node to use
   - Several endpoints, and the V1 client load-balances, discover nodes, or fails over (`ClickHouseNodes`, `ClickHouseCluster`, `load_balancing_policy`, `failover`, `auto_discovery`)
2. Which protocol does this code use?
   - HTTP or HTTPS
   - Native TCP or gRPC
3. How long does the V1 client live?
   - Created once and shared
   - Created for each query or insert (as in `examples/client/.../jdbc/Main.java`)
4. How does it authenticate and encrypt?
   - User and password over plain HTTP
   - User and password over HTTPS
   - Access token or bearer token
   - Client certificate or a custom CA

## Stop

Stop, and say why, when the answer or the code depends on:

- native TCP or gRPC (`ClickHouseProtocol` in `client-v2` is HTTP only)
- client-side load balancing, node discovery, or failover

Do not invent a V2 equivalent. Multiple `addEndpoint` calls are only for endpoints the application already lists. Say that V2 does not expose V1 node policies or a per-call node handle.

## Rewrite

- Build with `new Client.Builder()`, then `build()`.
- `ClickHouseNode` host, port, and `https` become `addEndpoint`. Use `https://` only when the user chose HTTPS or the V1 URL or `ssl` flag is already secure. `SSLMode` does not turn an `http://` endpoint into TLS.
- Credentials on the node or `defaultCredentials` become `setUsername` / `setPassword`, `setAccessToken`, or `useBearerTokenAuth`.
- Node database becomes `setDefaultDatabase`.
- A V2 `Client` is thread-safe and long-lived. Hoist per-call `ClickHouseClient.newInstance` to one shared client. Close it when the application shuts down (`AutoCloseable`), not after every operation.
- Keep separate clients when each call uses credentials that cannot share one client. Mention `updateUserAndPassword` only when the code already switches user and password on the same auth type.
- Drop `nodeSelector` protocol filters. V2 has no protocol plugin.

## Output

Return the rewritten construction, the assumptions for any skipped question, and a short note when endpoint, auth, or client lifetime changed.
