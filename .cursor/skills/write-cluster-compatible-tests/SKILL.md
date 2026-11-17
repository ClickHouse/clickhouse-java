---
name: write-cluster-compatible-tests
description: Write and fix ClickHouse integration tests in clickhouse-java so they pass under TEST_CLICKHOUSE_CONTAINER=cluster (3-replica cluster behind nginx with Keeper and a Replicated test database). Use when adding or fixing integration tests in client-v2, jdbc-v2, or clickhouse-client, or when a cluster run fails with ON CLUSTER errors, missing tables, missing rows, missing users/roles, or missing query_log entries.
---

# Write Cluster-Compatible Integration Tests

In cluster mode (`TEST_CLICKHOUSE_CONTAINER=cluster`), `ClusterClickHouseContainerStarter` starts 3 ClickHouse replicas (`clickhouse-1..3`, shard `s1`, cluster `test_cluster`), 1 `clickhouse-keeper`, and 1 `nginx` reverse proxy that routes each HTTP/TCP request to any replica.

`cluster_quorum.xml` sets `<default_table_engine>ReplicatedMergeTree</default_table_engine>` and `<insert_quorum>3</insert_quorum>` for the `default` user.

## Core Rule: `Replicated` Database vs. `Atomic` Database

How DDL must be written depends on **which database** the test targets:

| Database used by test | Engine in cluster mode | Table `CREATE` / `DROP` | Table `ENGINE` |
| --- | --- | --- | --- |
| `ClickHouseServerForTest.getDatabase()` (standard for `client-v2` / `jdbc-v2`) | `Replicated` | **Never** use `ON CLUSTER` (`Code: 80: ON CLUSTER is not allowed for Replicated database`) | Omit `ENGINE` (uses `ReplicatedMergeTree` default) or set `ReplicatedMergeTree` when `isCluster()` |
| `default` database | `Atomic` | **Must** use `onCluster()` (`ON CLUSTER 'test_cluster'`) | Omit `ENGINE` or set `ReplicatedMergeTree` when `isCluster()` |
| Custom DB created by test (`CREATE DATABASE db` + `onCluster()`) | `Atomic` | **Must** use `onCluster()` on `CREATE/DROP DATABASE` and every `CREATE/DROP TABLE` inside it | Omit `ENGINE` or set `ReplicatedMergeTree` when `isCluster()` |

### Why this matters

- `ClickHouseContainerStarter.createDatabase` creates `ClickHouseServerForTest.getDatabase()` on every node with `ENGINE = Replicated(...)`. ClickHouse replicates all DDL inside that database automatically and rejects `ON CLUSTER` with `INCORRECT_QUERY`.
- `TestDataSet.getCreateTableSql`:
  - Inside `ClickHouseServerForTest.getDatabase()`, call `dataSet.getCreateTableSql(table, null, isCluster())` and `DROP TABLE IF EXISTS <table> SYNC` (no `onCluster()`).
  - Inside `default` (or a custom `Atomic` DB), call `dataSet.getCreateTableSql(table, getClusterName())` and `DROP TABLE IF EXISTS <table>` + `onCluster()` + `SYNC`.

### Choosing the Table Engine

- Prefer omitting `ENGINE = ...` (`CREATE TABLE t (id Int32, ...) ORDER BY id`); single-node uses `MergeTree` and cluster uses `ReplicatedMergeTree`.
- When an explicit engine is required (e.g., `ReplacingMergeTree`), select the replicated variant in cluster mode:
  ```java
  String engine = isCluster() ? "ReplicatedReplacingMergeTree" : "ReplacingMergeTree";
  ```
- Avoid `ENGINE = MergeTree` or `ENGINE = Memory` for multi-statement tests: data inserted on one replica is invisible when `nginx` routes the `SELECT` to another replica.

## Users, Roles, Profiles, and Grants

The cluster uses a Keeper-backed `<replicated>` user directory that propagates asynchronously across replicas.

1. Use unique names per test (e.g., random suffix) and clean up in `finally`.
2. After `CREATE USER`, `CREATE ROLE`, `CREATE SETTINGS PROFILE`, or `GRANT`, wait for all 3 replicas before using or dropping them:

```java
client.execute("CREATE USER " + user + " IDENTIFIED BY '" + password + "'").get().close();
waitForAllReplicas("users", "name = '" + user + "'", 1,
        sql -> client.queryAll(sql).stream().mapToLong(r -> r.getLong("matching_rows")).toArray());
```

(In `clickhouse-client` / `jdbc-v2` tests with `HttpURLConnection`/v1 client on classpath, the 3-arg `BaseIntegrationTest.waitForAllReplicas(table, condition, expectedRows)` also works.)

## `SYSTEM` Commands and System Tables

- **Flushing logs:** Append `onCluster()` so every replica flushes before querying `system.query_log`:
  ```java
  client.execute("SYSTEM FLUSH LOGS" + onCluster()).get().close();
  ```
- **Querying node-local system tables (`system.query_log`, `system.processes`):** Use `clusterAllReplicas(...)` when `isCluster()` so you find the query regardless of which replica handled it:
  ```java
  String from = isCluster()
          ? "clusterAllReplicas('" + getClusterName() + "', system.query_log)"
          : "system.query_log";
  ```

## Tests Requiring Single-Server Affinity

Skip tests with `if (isCloud() || isCluster())` when they depend on hitting the exact same ClickHouse instance across separate requests:

- Server-side HTTP `session_id` / `session_check` / `CREATE TEMPORARY TABLE`
- WireMock or single-endpoint tests where `nginx` / cluster routing does not apply

## Verification

Run targeted integration tests against the cluster setup before finishing:

```bash
# If clickhouse-client test helpers (e.g. TestDataSet, BaseIntegrationTest) changed:
mvn -q --no-transfer-progress --batch-mode -pl clickhouse-client -DskipTests install

# Run targeted cluster integration tests:
TEST_CLICKHOUSE_CONTAINER=cluster TEST_CLICKHOUSE_IMAGE_VERSION=25.8 \
  mvn --no-transfer-progress --batch-mode --projects client-v2 \
  -Dit.test="MyClusterTest" -DfailIfNoTests=false -DskipUTs=true verify
```
