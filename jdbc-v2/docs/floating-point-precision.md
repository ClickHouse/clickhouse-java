# Floating-point precision in jdbc-v2

This note documents how `Float32` and `Float64` values behave when written through the
`jdbc-v2` driver, why a `Float32` value can come back differing by up to one ULP, and how
tests should assert floating-point round-trips.

## Background

`PreparedStatement` parameters in `jdbc-v2` are **not** sent in ClickHouse `RowBinary`
form. Instead the driver serializes each parameter into a **textual SQL literal** and
substitutes it into the statement before sending it to the server
(`com.clickhouse.jdbc.PreparedStatementImpl#encodeObject`).

For numeric primitives there is no dedicated branch, so the value falls through to the
default rendering:

```java
return SQLUtils.escapeSingleQuotes(x.toString());
```

This means:

- `setFloat(i, x)`  → `Float.toString(x)`  (e.g. `-3.402823E38`)
- `setDouble(i, x)` → `Double.toString(x)` (e.g. `-1.7976931348623157E308`)

`Float.toString` / `Double.toString` produce the *shortest* decimal string that round-trips
back to the **same** value *of that width* (`Float.parseFloat` for a float, `Double.parseDouble`
for a double).

## Why a `Float32` can shift by one ULP

When the server reads a decimal literal into a `Float32` column it parses it as `Float64`
first and then narrows the result to `Float32`. That is a **double rounding**:

```
decimal text  ->  Float64 (round #1)  ->  Float32 (round #2)
```

The shortest decimal emitted by `Float.toString` is only guaranteed to round-trip via a
**single** `decimal -> Float32` rounding. Routing it through `Float64` first can land on an
adjacent `Float32` value. The error is bounded by **one ULP** of the `Float32`, and in
practice only shows up for extreme magnitudes where the ULP is large.

Observed example:

| Java value inserted | Text sent     | Value read back   |
| ------------------- | ------------- | ----------------- |
| `-3.402823E38f`     | `-3.402823E38`| `-3.4028229E38f`  |

`Float64` columns are **not** affected: the literal is parsed straight to `Float64` with a
single rounding, and `Double.toString` already round-trips exactly through that path.

## Read side

On retrieval the binary reader returns boxed primitives that mirror the column type
(`com.clickhouse.client.api.data_formats.internal.BinaryStreamReader`):

| Column type | `getObject` returns |
| ----------- | ------------------- |
| `Float32`   | `java.lang.Float`   |
| `Float64`   | `java.lang.Double`  |

So `ResultSet#getObject("float32")` is a `Float`, and `getObject("float64")` is a `Double`.

## Implications for callers

- A `Float32` written via `setFloat` may read back differing by up to one ULP. If you need
  bit-exact `Float32` round-trips, avoid the text path — for example insert through the
  `client-v2` binary writer, or store the value in a `Float64` column.
- `Float64` written via `setDouble` round-trips exactly.

## Testing guidance

Floating-point round-trips should not be asserted with exact equality for `Float32`. Use a
one-ULP tolerance for `Float32` and exact equality for `Float64`:

```java
// Float32: allow up to one ULP because of decimal -> Float64 -> Float32 double rounding
assertEquals(rs.getFloat("float32"), expected32, Math.ulp(expected32));

// Float64: exact
assertEquals(rs.getDouble("float64"), Double.valueOf(expected64));
```

The same tolerance applies when comparing the boxed `Float` returned by `getObject`:

```java
Object actual32 = rs.getObject("float32");
assertTrue(actual32 instanceof Float);
assertEquals((float) (Float) actual32, expected32, Math.ulp(expected32));
```

See `com.clickhouse.jdbc.JdbcDataTypeTests#testFloatTypes` for a data-set-driven example
that exercises minimum, maximum, zero, unit, subnormal, constant, and random values.

## Related: ClickHouse Cloud read-after-write

`JdbcDataTypeTests` writes test rows and reads them back. On ClickHouse Cloud, separate
connections may be routed to different replicas, so a value written on one connection is not
guaranteed to be immediately visible to a `SELECT` on another. To keep these tests
deterministic without per-query consistency tuning, perform the write and the verifying read
on the **same** connection. `JdbcIntegrationTest#runQuery(String, Connection)` is provided to
run DDL on that same connection and avoid opening extra ones.
