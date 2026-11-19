# Instructions for agents: core SQL test harness

This harness runs the ClickHouse core SQL tests through clickhouse-java, with
the `client` (client-v2) and the `jdbc` (jdbc-v2) backend. See
[`README.md`](README.md) for how it works and how to run it. The root
[`AGENTS.md`](../../AGENTS.md) also applies.

## The goal

The goal is to find bugs in clickhouse-java. To pass more tests is not the
goal. A failed test is information. Do not hide it.

## Rules

1. **Find the cause of a failure before you do anything with the test.** A
   test fails for one of three causes:
   - **The client** (client-v2, jdbc-v2, or `clickhouse-data`, which they
     use). This is what the harness is for.
   - **The shim** (the program in this directory). It does not do what
     `clickhouse-client` does.
   - **The environment.** The test cannot pass through any client on this
     server (see [Environment causes](#environment-causes)).
2. **The default for a client bug is a fix in the client:** a pull request
   that fixes the module, with a test in that module (see "Testing
   expectations" in the root `AGENTS.md`). Do not fix a client bug in a pull
   request that changes the harness: one change per pull request. When an
   issue is filed for a group, put it on the `Issue:` line of the group.
3. **The default for a shim bug is a fix in the shim.** Make the shim copy
   `clickhouse-client` more exactly. Do not change the shim to hide a client
   bug, for example by another read method, a type-name workaround or a
   correction of a wrong value. The shim must use the client as a user does:
   `client` reads each value with `ClickHouseBinaryFormatReader.readValue(i)`
   in the default format of the client, and `jdbc` with
   `ResultSet.getObject(i)`.
4. **Remove a test from the allowlist only when it cannot pass for an
   environment cause** (see rules 6 and 7). Write the reason in the commit
   message.
5. **A test that fails because of a client bug or a shim gap stays
   visible.** Do not delete it. Put it in [`allowlist.txt`](allowlist.txt)
   as a known failure in the group of its cause: `# <test>  # [<id>]` when
   it fails on both backends, `<test>  # known=<backend>: [<id>]` when it
   fails on one backend only. Each group has a header that describes the
   bug, gives a small reproduction, names the backends, gives the number of
   tests per backend and has an `Issue:` line. Make a new group when no group
   fits; mark a shim group `shim:` in its header. See the README for the
   format and for how to run one group (`KNOWN_FAILURES=<id>`).
6. **A test that cannot pass on one backend** for an environment cause gets
   `# only=<backend>: <reason>`. Do not use this for a client bug: use a
   known failure.
7. **Do not list a test that cannot pass for an environment cause** on both
   backends.

## How to find the cause

Do these steps in this order. Build the shim once (`scripts/build.sh`); the
commands below use `SKIP_BUILD=1`.

1. **Run the test on both backends:**

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) SKIP_BUILD=1 tests/clickhouse-client/scripts/run-upstream-tests.sh
   UPSTREAM_TEST_LIST=<(echo <test>) SKIP_BUILD=1 CLICKHOUSE_CLIENT_CLI_IMPL=jdbc tests/clickhouse-client/scripts/run-upstream-tests.sh
   ```

   Both backends use HTTP, so a test that passes on one backend and fails
   on the other points to the client (client-v2 or jdbc-v2), unless the
   cause is in the list below.
2. **Run the test through the real `clickhouse-client`** in the server
   container, on the same server:

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) tests/clickhouse-client/scripts/run-with-real-client.sh
   ```

   If it also fails here, the cause is the environment. If it passes here,
   the cause is the client or the shim.
3. **Find the statement that fails.** Set `CLICKHOUSE_CLIENT_CLI_LOG` to a
   file; the log shows each statement and the error. Read the diff in the
   output of the runner, and the `.stdout` and `.stderr` files next to the
   test.
4. **Compare the shim with the server on that one statement.** Run it
   through the shim (`tests/clickhouse-client/bin/clickhouse-client --query "<SQL>"`,
   on both backends) and through the real client
   (`docker exec clickhouse clickhouse-client --query "<SQL>"`). Then make the
   statement smaller until it is as small as possible.
5. **Decide between the client and the shim with a small Java program** that
   uses only the public API: client-v2 `Client.query` and
   `ClickHouseBinaryFormatReader.readValue` (or `getString`, `getLong`, ...),
   jdbc-v2 `ResultSet.getObject`. A TestNG test in the module is a good
   form: it can become the test of the fix. If the program also gives a
   wrong value or an error, the bug is in the client. If the program gives
   the correct value, the bug is in the shim.

### Environment causes

A test cannot pass through any client on the CI server when it needs one of
these:

- A cluster, shards or replicas (`CLUSTER_DOESNT_EXIST`, `test_shard_localhost`).
- ZooKeeper or Keeper (`NO_ZOOKEEPER`, `Replicated*` engines, `KeeperMap`).
- External services: S3, Azure, HDFS, Kafka, MySQL, PostgreSQL, MongoDB, URL
  sources.
- Server configuration of the upstream CI: named collections, dictionaries,
  storage policies, disks, users and grants, macros, transactions
  (`NAMED_COLLECTION_DOESNT_EXIST`, `NO_ELEMENTS_IN_CONFIG`, `UNKNOWN_POLICY`,
  `UNKNOWN_DISK`).
- Features that the server build or platform does not have
  (`SUPPORT_IS_DISABLED`).
- Stateful datasets (`--no-stateful`) or long tests (`--no-long`).
- Behavior of the HTTP interface itself. Both backends use HTTP. The server
  sends no `WITH TOTALS` and no `extremes` rows in the binary output over
  HTTP, and an HTTP request fails when the current database of the session
  was dropped.
- Features of `clickhouse-client` itself that are not part of a driver:
  `INTO OUTFILE`, `FROM INFILE`, client-side parsing of `INSERT` data, and
  client-only settings.
- The tz database. The JVM and ClickHouse can give different historical
  offsets for the same zone.
- Tests that conflict with other tests of the set: a fixed table UUID, or
  server state that a test leaves behind (failpoints that a failed test did
  not disable, row policies, databases). A failure message with "Injected
  failure" comes from a failpoint of another test.
- Server results that depend on the load of the server: the pipeline shape
  of `EXPLAIN PIPELINE` or `EXPLAIN ANALYZE` (the number of streams, read in
  order or not), block sizes, and the total memory of the server
  (`MEMORY_LIMIT_EXCEEDED` with "(total)"). Such a test passes alone on a
  fresh server and fails in a full run, also through the real
  `clickhouse-client`. The shim only sends the SQL text, so the shim and the
  client cannot change these results.
- Memory limits of the server, when the test also fails through the real
  `clickhouse-client`.

These are **not** environment causes. Find the bug:

- A value that is different from the server's value.
- A decode error or a client exception (`ClientException`, `SQLException`
  from `getObject`, "Failed to read value").
- A type name that the client cannot parse or gives wrong.
- A failure on one backend only, when the cause is not in the list above.

## Conditions for the allowlist

- Add a test only when it passes on **both** backends, or on one backend
  with an `only=` reason (rule 6), or on one backend with a `known=` group
  for the other (rule 5).
- Run new tests three times, also with a higher `-j` than CI (CI uses
  `-j 4`), on a **fresh** server (`docker rm -fv clickhouse`, then start it
  again as in the README). Tests change the state of the server (row
  policies, settings profiles, users). A test that passes only after other
  tests ran must not go in.
- Run the full list for the checks, not only the new tests: a test that
  passes alone can fail in a full run (see [Environment causes](#environment-causes)).
- Do not run two sweeps at the same time on one server. The memory pressure
  causes `MEMORY_LIMIT_EXCEEDED` failures that are not real.
- CI does not retry failed tests (`RETRY_FAILED` is not set). Do not use the
  retry when you grow or check the allowlist.
- When a test in the allowlist starts to fail intermittently, find the cause
  first (rule 1). Remove it only when the cause is in the environment or in
  the test itself, and write the reason.

## Bump the server version

The server version and the upstream tests are pinned together in
[`clickhouse-version`](clickhouse-version). A new ClickHouse release changes
nothing in CI until a pull request bumps the pin, so other pull requests do
not fail because of a release. Bump the pin in a pull request of its own,
and make that pull request pass before it merges:

1. Select the version: a full version with a server image tag and a
   `v<version>-stable` tag upstream, normally the version of the `latest`
   image.
2. Try it: start a manual run with the `clickhouse_version` input, or run
   the allowlist locally on that version (`fetch-upstream.sh <version>`, and
   a fresh server of that version).
3. For each allowlisted test that now fails, find the cause (rule 1). A
   release changes the tests and the server output, so many tests can fail
   at the same time with no change in the client. To see whether the client
   is the cause, run the same test through the real client
   (`run-with-real-client.sh`) on the same server.
4. Run the known failures on both backends (`KNOWN_FAILURES=all`). Enable the
   tests that pass now.
5. Fix the names of allowlisted tests that the run reports as not in the
   checkout (upstream renamed or removed them).
6. Optionally, try the new tests of the release (see "Grow the allowlist" in
   the README).
7. Change `clickhouse-version`, run `fetch-upstream.sh`, and commit
   `src/main/resources/com/clickhouse/client/cli/error-codes.tsv` with it.

## Keep the documents in sync

When you change the behavior of the shim (flags, statement handling, output
formats, settings, environment variables), update [`README.md`](README.md),
this file, the usage text of the shim (`CliOptions.usage`) and the workflow
in the same change.
