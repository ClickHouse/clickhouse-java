# clickhouse-client-cli: the core SQL tests through clickhouse-java

This harness runs the ClickHouse core SQL tests
([`tests/queries/0_stateless`](https://github.com/ClickHouse/ClickHouse/tree/master/tests/queries/0_stateless))
through clickhouse-java.

The upstream test runner
([`tests/clickhouse-test`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/clickhouse-test))
runs each `.sql` test through `clickhouse-client` and compares the output with
the `.reference` file of the test. This harness is a small Java program (the
shim) that imitates `clickhouse-client` well enough for the runner to drive it
without changes: the same flags, the same stdin and `--query` behavior, and the
`extract-from-config` shortcut.

The shim sends every statement through clickhouse-java, built from the source
of this repository. It reads each value with the normal type mapping of the
client and writes the values as `TabSeparated` text itself. Thus a test passes
only when the client decodes every value correctly. The environment variable
`CLICKHOUSE_CLIENT_CLI_IMPL` selects the backend:

| Backend | Client | Values from | Column types from |
|---|---|---|---|
| `client` (default) | client-v2 | `ClickHouseBinaryFormatReader.readValue(i)` | `ClickHouseColumn.getOriginalTypeName()` |
| `jdbc` | jdbc-v2, through `DriverManager` | `ResultSet.getObject(i)` | `ResultSetMetaData.getColumnTypeName(i)` |
| `text` | client-v2 | the text output of the server, copied as it is | — |

`text` checks only the transport, not the decoding. CI does not run it.

clickhouse-go and clickhouse-js have harnesses with the same design
(`tests/clickhouse-test-runner` in each repository).

## Run the tests locally

You need JDK 17 and Maven with a toolchain for JDK 17 (see
[`CONTRIBUTING.md`](../../CONTRIBUTING.md)), Python 3 with `jinja2`, Docker,
bash, git and curl. The commands run from the root of the repository.

1. Start the pinned ClickHouse server. The version is in
   [`clickhouse-version`](clickhouse-version). The container name `clickhouse`
   is the default of `run-with-real-client.sh`. CI starts the same server:

   ```bash
   docker run -d --name clickhouse -p 8123:8123 -p 9000:9000 --hostname "$(hostname)" \
     --ulimit nofile=262144:262144 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
     "clickhouse/clickhouse-server:$(tests/clickhouse-client/scripts/pinned-version.sh)"
   ```

   `CLICKHOUSE_SKIP_USER_SETUP=1` keeps the `default` user of the image
   (network access, and the access management that tests need). The host
   name of the runner is necessary because `tests/clickhouse-test` replaces
   its own host name with `localhost` in the output of each test. For a fresh
   server, run `docker rm -fv clickhouse` and start it again.

2. Get the upstream tests of the same version:

   ```bash
   tests/clickhouse-client/scripts/fetch-upstream.sh
   ```

   The script checks out the release tag of the pinned version (for example
   `v26.10.4.207-stable`) into `tests/clickhouse-client/.upstream/ClickHouse`,
   sparse and shallow. The `.reference` files of that tag were made by the
   same server build. The script also generates the error code table of the
   shim (`src/main/resources/com/clickhouse/client/cli/error-codes.tsv`) from
   the same checkout. To try another version, give it as the argument
   (`fetch-upstream.sh 26.11.1.10`), and start the server of that version.
   That also changes `error-codes.tsv`: restore the file after the try
   (`git checkout -- <file>`), unless you bump the version.

   The run script stops when the server and the tests are of different
   versions, because then the `.reference` files do not match. Set
   `ALLOW_VERSION_MISMATCH=1` to run anyway.

3. Run the allowlisted tests, one backend after the other:

   ```bash
   pip install jinja2
   tests/clickhouse-client/scripts/run-upstream-tests.sh -j 4
   CLICKHOUSE_CLIENT_CLI_IMPL=jdbc SKIP_BUILD=1 tests/clickhouse-client/scripts/run-upstream-tests.sh -j 4
   ```

The run script first builds the shim with `scripts/build.sh`: it installs
`clickhouse-jdbc` and the modules that it needs (`jdbc-v2`, `client-v2`,
`clickhouse-data`, ...) from the current source into the local Maven
repository, and packages the shim against exactly that version
(`target/clickhouse-client-cli-1.0.0.jar`). `SKIP_BUILD=1` uses the jar that is
already there. Then the script makes a class data sharing archive for the
backend, puts [`bin/`](bin) first in `PATH` for the runner only, and starts
`tests/clickhouse-test` with
`--no-stateful --no-random-settings --no-random-merge-tree-settings`. Other
arguments go to the runner after these. For example, to run one test that is
not in the allowlist:

```bash
UPSTREAM_TEST_LIST=<(echo 00001_select_1) SKIP_BUILD=1 tests/clickhouse-client/scripts/run-upstream-tests.sh
```

The runner writes the diff of a failed test to its own output, and the
output of the test next to the test, in
`.upstream/ClickHouse/tests/queries/0_stateless/<test>.<pid>.stdout` and
`.stderr` (without `.<pid>` when the test does not run in parallel).

To run one statement through the shim, after a build:

```bash
tests/clickhouse-client/bin/clickhouse-client --query "SELECT 1"
CLICKHOUSE_CLIENT_CLI_IMPL=jdbc tests/clickhouse-client/bin/clickhouse-client --query "SELECT 1"
```

The unit tests of the shim need no server. They run in each
`scripts/build.sh` run (the run script builds with `-DskipTests`). The live
tests compare the output of the shim with the server's own `TabSeparated`
output for many expressions, on both backends, and need the server of step 1.
After a first build, `SKIP_CLIENT_INSTALL=1` uses the clickhouse-java modules
that the build installed:

```bash
CLICKHOUSE_CLIENT_CLI_LIVE=1 SKIP_CLIENT_INSTALL=1 tests/clickhouse-client/scripts/build.sh
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `CLICKHOUSE_CLIENT_CLI_IMPL` | `client` | The backend: `client`, `jdbc` or `text`. |
| `CLICKHOUSE_CLIENT_CLI_LOG` | (none) | A file. Each invocation of the shim adds its arguments (the `--password` value hidden), statements and errors to it. |
| `UPSTREAM_CLICKHOUSE_DIR` | `.upstream/ClickHouse` | The checkout of `ClickHouse/ClickHouse`. |
| `UPSTREAM_REPO_URL` | `ClickHouse/ClickHouse` on GitHub | The repository that `fetch-upstream.sh` checks out. |
| `UPSTREAM_TEST_LIST` | `allowlist.txt` | The file with the tests to run. |
| `SHARD_INDEX`, `SHARD_TOTAL` | `1`, `1` | Run only shard `SHARD_INDEX` of `SHARD_TOTAL` round-robin shards of the list. |
| `KNOWN_FAILURES` | (none) | Run the known failures of these groups (comma-separated IDs, or `all`) instead of the allowlist. See [The allowlist](#the-allowlist). |
| `RETRY_FAILED` | `0` | Run the failed tests again, at most this number of times. See [CI](#ci). |
| `ALLOW_VERSION_MISMATCH` | `0` | `1`: run also when the server and the upstream tests are of different versions. |
| `SKIP_BUILD` | `0` | `1`: use the jar in `target/`, do not build. |
| `SKIP_CLIENT_INSTALL` | `0` | `1`: `build.sh` packages the shim against the clickhouse-java modules that are already in the local Maven repository. |
| `CLICKHOUSE_HOST` | `localhost` | The server for the run script and the runner. |
| `CLICKHOUSE_PORT_TCP`, `CLICKHOUSE_PORT_HTTP` | `9000`, `8123` | The ports of the server. When `CLICKHOUSE_PORT_TCP` is set, the runner gives `--port=$CLICKHOUSE_PORT_TCP`; the shim uses `CLICKHOUSE_PORT_HTTP` for it, and also when there is no `--port`. |
| `CLICKHOUSE_CLIENT_CLI_CDS` | `target/clickhouse-client-cli-<backend>.jsa` | The class data sharing archive of the shim JVM. An empty value disables it. |
| `CLICKHOUSE_CLIENT_CLI_JAVA_OPTS` | (none) | More options for the shim JVM (a later `-Xmx` wins over the default `-Xmx384m`). |
| `CLICKHOUSE_CONTAINER` | `clickhouse` | The server container of `run-with-real-client.sh`. |
| `CLICKHOUSE_CLIENT_CLI_LIVE`, `CLICKHOUSE_CLIENT_CLI_LIVE_HOST` | (none), `localhost` | `1`: the build also runs the live tests, against the server on this host (HTTP port 8123). |

## The allowlist

[`allowlist.txt`](allowlist.txt) lists the tests that pass reliably, one test
name (the file name without `.sql` or `.sql.j2`) per line. `#` starts a
comment. The script gives the runner anchored regular expressions, so each
name selects exactly one test. (The runner itself matches each name as a
substring, so `00396_uuid` would also select `00396_uuid_v7`.)

The file has these kinds of lines:

| Line | Meaning |
|---|---|
| `00001_select_1` | The test runs on both backends. |
| `00571_...  # only=jdbc: <reason>` | The test runs on the `jdbc` backend only, because it cannot pass on the other backend for an environment cause. |
| `# 01601_accurate_cast  # [type-name-nullable]` | A **known failure** on both backends: the test does not run, because of a bug in clickhouse-java or a gap in the shim. The ID in brackets names the group. |
| `05210_...  # known=jdbc: [jdbc-use-database]` | A known failure on the `jdbc` backend only. The test runs on `client`. |
| `02457_...  # known=client: [type-empty-tuple]  # known=jdbc: [jdbc-interval-getobject]` | A known failure on both backends, with a different cause on each. The test does not run. |

The known failures are at the end of the file, in one section per group. The
header of a group describes the bug, gives a small reproduction, names the
backends, gives the number of tests per backend, and has an `Issue:` line.
When an issue is filed for a group, put its link on that line. Make a new
group when no group fits, and mark a shim group `shim:` in its header. A
line with a group per backend is in the section of its first group; the
header of the other group names it. The comment at the start of the file gives
the counts. Update them when you change the list: the run script prints
`Selected <N> test(s)` for each backend.

To run the known failures of one or more groups, for example to check a fix:

```bash
KNOWN_FAILURES=type-empty-tuple tests/clickhouse-client/scripts/run-upstream-tests.sh
KNOWN_FAILURES=all CLICKHOUSE_CLIENT_CLI_IMPL=jdbc tests/clickhouse-client/scripts/run-upstream-tests.sh
```

`KNOWN_FAILURES` selects only the known failures of the backend of the run.
When a fix makes the tests of a group pass, remove the `# ` at the start and
the `# [<id>]` at the end of their lines (or the `# known=<backend>: [<id>]`
part). When all tests of a group pass, remove the group header.

### Grow the allowlist

1. Run the candidate tests through the harness on **both** backends, one
   after the other, on a fresh server. To try all `.sql` tests:

   ```bash
   ls tests/clickhouse-client/.upstream/ClickHouse/tests/queries/0_stateless \
     | sed -nE 's/\.sql(\.j2)?$//p' | sort -u > /tmp/candidates.txt
   for b in client jdbc; do
     CLICKHOUSE_CLIENT_CLI_IMPL=$b UPSTREAM_TEST_LIST=/tmp/candidates.txt SKIP_BUILD=1 \
       tests/clickhouse-client/scripts/run-upstream-tests.sh -j 6 --no-long --timeout 120 \
         --max-failures-chain 1000000 > /tmp/run-$b.log 2>&1
   done
   ```

   A full run takes about 25 minutes per backend at `-j 6`. Give the server at
   least 8 GiB of memory: with 4 GiB, the kernel stopped it after about 12,600
   tests. Do not run two sweeps on one server at the same time: the memory
   pressure causes `MEMORY_LIMIT_EXCEEDED` failures that are not real.
   `--max-failures-chain` turns off a limit of the runner: by default it stops
   after 20 failures in a row, which a list of failing tests reaches at once.

2. Compare the backends:

   ```bash
   ok() { sed -nE 's/^.*\] ([^ :]+): +\[ OK \].*/\1/p' "$1" | sort -u; }
   comm -12 <(ok /tmp/run-client.log) <(ok /tmp/run-jdbc.log)   # OK on both backends
   comm -23 <(ok /tmp/run-client.log) <(ok /tmp/run-jdbc.log)   # OK on client only
   comm -13 <(ok /tmp/run-client.log) <(ok /tmp/run-jdbc.log)   # OK on jdbc only
   ```

   A test that passes on both backends is a candidate. For a test that
   passes on one backend only, find the cause of the failure on the other
   one (see [Find the cause of a failure](#find-the-cause-of-a-failure)): a
   client bug or a shim gap gives a `known=<backend>` line, an environment
   cause an `only=<backend>` line.

3. Run the new tests again, three times, with a higher `-j` than CI (CI uses
   `-j 4`), each time on a fresh server. Tests change the state of the server
   (row policies, settings profiles, users): a test that passes only after
   other tests ran must not go in. Find the cause of each failure before you
   remove a test.

4. Run the full list for the checks, not only the new tests: a test that
   passes alone can fail in a full run (see
   [Environment causes](#environment-causes)).

**Rule:** add only tests that pass reliably: on both backends, or on one
backend with an `only=` reason or a `known=` group for the other. Before you
remove a test or decide not to add it, find out whether it fails because of a
bug in clickhouse-java. The harness exists to find those bugs. When an
allowlisted test starts to fail intermittently, find the cause first. Remove
it only when the cause is in the environment or in the test itself, and write
the reason in the commit message.

To compare with the real `clickhouse-client`, run the same tests through it,
in the server container:

```bash
UPSTREAM_TEST_LIST=<(echo 01601_accurate_cast) tests/clickhouse-client/scripts/run-with-real-client.sh
```

A test that passes there and fails through clickhouse-java shows a bug in
clickhouse-java or in the shim.

## Find the cause of a failure

A test fails because of the client (client-v2, jdbc-v2, or `clickhouse-data`,
which they use), the shim, or the environment. [`AGENTS.md`](AGENTS.md) gives
the rules for each cause. Do these steps in this order. Build the shim once
(`scripts/build.sh`); the commands below use `SKIP_BUILD=1`.

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
   form: it can become the test of the fix (see "Testing expectations" in the
   root [`AGENTS.md`](../../AGENTS.md)). If the program also gives a wrong
   value or an error, the bug is in the client. If the program gives the
   correct value, the bug is in the shim.

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

## How the shim imitates clickhouse-client

The shim sends nothing that `clickhouse-client` does not send, because some
tests read their own queries from `system.query_log`.

- **Statements.** The shim splits the input into statements as
  `clickhouse-client` does: at a top-level `;`, with the same-line trailing
  comment kept with its statement. It sends each statement with its `;`. The
  inline data of `INSERT ... FORMAT <name>` (other than `Values`) ends at an
  empty line, and goes to the server with its statement. The `-- Tags:`
  header is not part of a statement. Every statement runs, with or without
  `--multiquery`.
- **Test hints.** `-- { serverError X }`, `-- { clientError X }` and
  `-- { error X }` (codes or names) make the error the expected result. An
  error hint in a leading comment is ignored, as upstream. Through clickhouse-java the
  server finds all errors, so a server error can satisfy a `clientError`
  hint. `-- { echo }`, `-- { echoOn }` and `-- { echoOff }` print the
  statements, but not a statement with a syntax error (`clickhouse-client`
  parses before it echoes). A `-- { retry ... }` hint stops the shim with an
  error.
- **Sessions.** `SET`, `USE` and temporary tables last for the whole
  invocation. Both backends use HTTP, with one `session_id` for all requests
  of an invocation. The `client` backend sends the current database with
  each query (`QuerySettings.setDatabase`); the shim reads the database of a
  `USE` statement. The `jdbc` backend leaves `USE` to the driver.
- **Statement types.** Each statement runs with `Client.query` (`client`) or
  `Statement.execute` (`jdbc`); a result set is written as text. After
  `SET dialect` to another language (KQL, PRQL, ...), the shim removes the
  trailing `;` of a statement, because the server reads the HTTP request as
  one query.
- **Output formats.** The client reads results in a binary format
  (`RowBinaryWithNamesAndTypes`; the server prefers the format header of the
  request to a `FORMAT` clause), and the shim writes them itself in the
  `TabSeparated` family (also `TSVRaw` and `...WithNames[AndTypes]`) or
  `Null`. A statement with any other output format fails
  (`[shim-output-format]` in the allowlist). With
  `ignore_format_null_for_explain` (on by default), `EXPLAIN ... FORMAT Null`
  prints the plan, as the server does.
- **Rendering.** The renderer is type-directed: it uses the column type of
  the result metadata, and writes each value as the server does in
  `TabSeparated`: escaped strings and `\N` at the top level, quoted strings
  and `NULL` in `Array`, `Tuple` and `Map`, the shortest float text, decimals
  without trailing zeros, `DateTime64(P)` with P digits, `Time` over 24 hours,
  the server's IPv6 forms, and the map order of the server. `WITH TOTALS` and
  `extremes` rows are not in the binary output over HTTP.
- **Settings.** Of the settings on the command line, only `log_comment`,
  `max_insert_threads`, `max_threads`, `send_logs_level`, `async_insert` and
  `enable_parallel_replicas` go to the server, with each query, as in
  `clickhouse-client`. (The runner sets the last two to 0 for the tests with
  the tags `no-async-insert` and `no-parallel-replicas`.) The `client` backend
  sends them as server settings of each query; the `jdbc` backend as
  connection properties (`clickhouse_setting_<name>`). A `SET` of the same
  setting in the test replaces the command-line value. `--param_<name>=<value>`
  gives a query parameter.
- **Ports.** When `CLICKHOUSE_PORT_TCP` is set, the runner gives that native
  port (`--port=$CLICKHOUSE_PORT_TCP`). The shim uses `CLICKHOUSE_PORT_HTTP`
  instead, and also when there is no `--port` (with `--secure`,
  `CLICKHOUSE_PORT_HTTPS` for `CLICKHOUSE_PORT_TCP_SECURE`). Any other port is
  used as given.
- **`extract-from-config`.** [`bin/clickhouse`](bin/clickhouse) answers
  `clickhouse extract-from-config --key listen_host` with `127.0.0.1`, without
  a JVM. Other keys give no output.
- **Errors.** An unexpected error stops the shim. It writes the error with its
  code and name to stderr (`Code: N. DB::Exception: ... (NAME)`), and the exit
  code is the error code modulo 256. The shim writes nothing else to stderr:
  the runner fails a `.sql` test that writes to stderr. The client logs
  through `slf4j-nop`, and the JVM log output is off.
- **Error names.** `error-codes.tsv` maps the error names of test hints to
  codes. [`scripts/fetch-upstream.sh`](scripts/fetch-upstream.sh) generates it
  from `src/Common/ErrorCodes.cpp` of the upstream checkout, so it always
  matches the tests. Commit the file when it changes.
- **JVM start.** The runner starts the shim once per test, so the JVM must
  start fast: [`bin/clickhouse`](bin/clickhouse) runs the fat jar with C1
  only, the serial collector, a small heap (`-Xmx384m`), and the class data
  sharing archive that the run script makes.

## CI

[`upstream-sql-tests.yml`](../../.github/workflows/upstream-sql-tests.yml)
runs the allowlist nightly, manually, and on pushes and pull requests that
change this directory or the workflow.

- The `build` job builds the shim against the source of the commit, runs its
  unit and live tests on the pinned server, and gives the jar to the test
  jobs.
- The test jobs are `{backend: client, jdbc} × {shard: 1..8}`, each with its
  own fresh server, at `-j 4`. A shard has about 1,260 tests, and takes about
  3.5 minutes on 4 CPUs (measured with the server and the shim on 4 CPUs of a
  development host). If a shard takes
  much more than a few minutes, increase the number of shards: change the
  `shard` list and `SHARD_TOTAL` together.
- CI does not set `RETRY_FAILED`: no known client bug makes random tests fail,
  so a failed test fails the job. The script also turns off the runner's own
  retries (`--dont_retry_failures`). If such a bug appears, set `RETRY_FAILED`
  in the workflow and name the group of the bug in the comment there. A test
  that passes on a retry is then a warning; a test that fails on every
  attempt fails the job. Remove the retry when the bug is fixed. Do not use
  `RETRY_FAILED` when you grow or check the allowlist: a test that needs a
  retry is not reliable.
- On a failure, the job uploads the `.stdout` and `.stderr` files of the
  failed tests and the log of the shim (`upstream-run.log`). The diffs are in
  the job log.
- The server version and the upstream tests are pinned together in
  [`clickhouse-version`](clickhouse-version). A new ClickHouse release
  changes nothing in CI, so a pull request never fails because of a release
  that it does not touch. A bump is a pull request of its own; see
  [Bump the server version](#bump-the-server-version).
- The `clickhouse_version` input of a manual run tries another version
  without a commit, for example before a bump.
- When an allowlisted test is not in the checkout (upstream renamed or
  removed it), the run shows a warning with the names.

## Bump the server version

The server version and the upstream tests are pinned together in
[`clickhouse-version`](clickhouse-version). Bump the pin in a pull request of
its own, and make that pull request pass before it merges:

1. Select the version: a full version with a server image tag and a
   `v<version>-stable` tag upstream, normally the version of the `latest`
   image.
2. Try it: start a manual run with the `clickhouse_version` input, or run
   the allowlist locally on that version (`fetch-upstream.sh <version>`, and
   a fresh server of that version).
3. For each allowlisted test that now fails, find the cause (see
   [Find the cause of a failure](#find-the-cause-of-a-failure)). A release
   changes the tests and the server output, so many tests can fail at the
   same time with no change in the client. To see whether the client is the
   cause, run the same test through the real client
   (`run-with-real-client.sh`) on the same server.
4. Run the known failures on both backends (`KNOWN_FAILURES=all`). Enable the
   tests that pass now.
5. Fix the names of allowlisted tests that the run reports as not in the
   checkout (upstream renamed or removed them).
6. Optionally, try the new tests of the release (see
   [Grow the allowlist](#grow-the-allowlist)).
7. Change `clickhouse-version`, run `fetch-upstream.sh`, and commit
   `src/main/resources/com/clickhouse/client/cli/error-codes.tsv` with it.
