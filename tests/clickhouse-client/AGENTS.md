# Instructions for agents: core SQL test harness

This harness runs the ClickHouse core SQL tests through clickhouse-java, with
the `client` (client-v2) and the `jdbc` (jdbc-v2) backend.
[`README.md`](README.md) tells how it works, how to run it, and how to find
the cause of a failure.

The goal is to find bugs in clickhouse-java, not to pass more tests. A failed
test is information. Do not hide it.

## Rules

1. **Find the cause of a failure before you do anything with the test**
   ([steps](README.md#find-the-cause-of-a-failure)). The cause is the client
   (client-v2, jdbc-v2, or `clickhouse-data`), the shim (the program in this
   directory), or the environment (the test cannot pass through any client on
   this server, see [Environment causes](README.md#environment-causes)).
2. **Fix a client bug in the client**, in a pull request of its own with a
   test in the module. Do not fix it in a pull request that changes the
   harness.
3. **Fix a shim bug in the shim**: make it copy `clickhouse-client` more
   exactly. Do not change the shim to hide a client bug (another read method,
   a type-name workaround, a correction of a wrong value). `client` reads each
   value with `ClickHouseBinaryFormatReader.readValue(i)`, `jdbc` with
   `ResultSet.getObject(i)`.
4. **A test that fails because of a client bug or a shim gap stays visible**
   in [`allowlist.txt`](allowlist.txt), as a known failure in the group of its
   cause. Do not delete it. See [The allowlist](README.md#the-allowlist) for
   the format.
5. **Only an environment cause removes a test** from the allowlist, or gives
   it `# only=<backend>: <reason>` when it fails on one backend only. Write
   the reason in the commit message.
6. **Add only tests that pass reliably**
   ([Grow the allowlist](README.md#grow-the-allowlist)).
7. **Bump the server version in a pull request of its own**
   ([Bump the server version](README.md#bump-the-server-version)).
8. **Keep the documents in sync.** When you change the behavior of the shim
   (flags, statement handling, output formats, settings, environment
   variables), update `README.md`, the usage text of the shim
   (`CliOptions.usage`) and the workflow in the same change.
