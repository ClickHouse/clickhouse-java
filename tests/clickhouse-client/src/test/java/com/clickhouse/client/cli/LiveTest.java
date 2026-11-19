package com.clickhouse.client.cli;

import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests against a ClickHouse server: {@code CLICKHOUSE_CLIENT_CLI_LIVE=1}, and the server at
 * {@code CLICKHOUSE_CLIENT_CLI_LIVE_HOST} (default localhost), HTTP port 8123.
 */
public class LiveTest {

    private static final String[] BACKENDS = {"client", "jdbc"};
    private String host;

    @BeforeClass
    public void requireLive() {
        if (!"1".equals(System.getenv("CLICKHOUSE_CLIENT_CLI_LIVE"))) {
            throw new SkipException("set CLICKHOUSE_CLIENT_CLI_LIVE=1 to run the tests against a ClickHouse server");
        }
        String h = System.getenv("CLICKHOUSE_CLIENT_CLI_LIVE_HOST");
        host = h == null || h.isEmpty() ? "localhost" : h;
    }

    /** Returns the server's own TabSeparated text for a query. */
    private String serverTsv(String query) throws Exception {
        URI uri = URI.create("http" + "://" + host + ":8123/?default_format=TabSeparated&query="
                + URLEncoder.encode(query, StandardCharsets.UTF_8));
        HttpResponse<byte[]> res = HttpClient.newHttpClient()
                .send(HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
        String body = new String(res.body(), StandardCharsets.UTF_8);
        assertEquals(res.statusCode(), 200, body);
        return body;
    }

    private static final class ShimResult {
        final String stdout;
        final String stderr;
        final int code;

        ShimResult(String stdout, String stderr, int code) {
            this.stdout = stdout;
            this.stderr = stderr;
            this.code = code;
        }
    }

    private ShimResult runShim(String backend, String query, String... extraArgs) {
        Map<String, String> env = new HashMap<>();
        env.put(Main.IMPL_ENV, backend);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        List<String> args = new ArrayList<>(List.of("--host", host, "--query", query));
        args.addAll(List.of(extraArgs));
        int code = Main.run(args.toArray(new String[0]),
                new ByteArrayInputStream(new byte[0]), out, new PrintStream(err, true), env::get);
        return new ShimResult(new String(out.toByteArray(), StandardCharsets.UTF_8),
                new String(err.toByteArray(), StandardCharsets.UTF_8), code);
    }

    private static final String[] EXPRESSIONS = {
            "toInt8(-1), toUInt8(255), toInt64(-9223372036854775808), toUInt64(18446744073709551615)",
            "toInt16(-32768), toUInt16(65535), toInt32(-2147483648), toUInt32(4294967295)",
            "toInt128(-170141183460469231731687303715884105728), toUInt256(115792089237316195423570985008687907853269984665640564039457584007913129639935)",
            "arrayJoin([1e15, 1e21, 1.5e-7, 0.1 + 0.2, nan, inf, -inf, -0., 5e-324, 2.82879384806159e17])",
            "toFloat32(0.1), toFloat32(3.4028235e38), toFloat32(-1e-7), toBFloat16(1.5)",
            "toDecimal32(1.50, 2), toDecimal64(-0.001, 5), toDecimal128(100, 3), toDecimal256('1.5', 10), toDecimal32(0, 3)",
            "'a\\tb\\\\c\\nd''e\\0\\b\\f\\v\\x01\\r', toFixedString('ab', 4), 'ünïcödé'",
            "true, false, NULL, CAST(NULL AS Nullable(String)), toNullable(1)",
            "toDate('2020-01-02'), toDate32('1900-01-01'), toDate32('2299-12-31'), toDateTime('2020-01-01 10:00:00', 'UTC')",
            "toDateTime('2020-01-01 10:00:00', 'Asia/Tokyo'), toDateTime64('2020-01-01 00:00:00.120', 3), toDateTime64('2020-01-01 00:00:00', 9, 'Asia/Tokyo')",
            "toDateTime64('2019-03-31 02:30:00.5', 1, 'Europe/Amsterdam'), toDateTime('1970-01-01 00:00:00', 'UTC'), toDateTime64('1900-01-01 00:00:00.123456', 6, 'UTC')",
            "toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toIPv4('1.2.3.4'), toIPv6('::ffff:1.2.3.4'), toIPv6('::1.2.3.4'), toIPv6('2001:db8::1'), toIPv6('::')",
            "[1, 2], ['a''b', NULL], [[1], []], [toDate('2020-01-01')], [toDateTime64('2020-01-01 00:00:00.5', 2)], []",
            "(1, 'x', NULL), CAST((1, 'x') AS Tuple(a Int32, b String)), [(1, 'a')], tuple(1)",
            "map('k', 1, 'a', 2), map(1, [2]), map('x', map('y', 1)), map()",
            "CAST('a' AS Enum8('a' = 1, 'b' = 2)), [CAST('b' AS Enum16('a' = 1, 'b' = 2))]",
            "toLowCardinality('x'), toLowCardinality(toNullable('y')), [toLowCardinality('z')], toLowCardinality(CAST(NULL AS Nullable(String)))",
            "(1.5, 2.5)::Point, [(1, 2), (3, 4)]::Ring",
            // Time values that a java.sql.Time without a fraction holds (jdbc-v2): in [0, 24 h), fraction 0.
            "toTime('12:34:56'), toTime64('10:10:10', 3), toTime64('23:59:59', 6), toTime('00:00:00')",
            "readWKT('POINT (1.5 -2)'), readWKT('LINESTRING (1 2, 3 4)'), readWKT('POLYGON ((0 0, 1e-7 0, 1 1, 0 0))')",
            "readWKT('MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((5 5, 6 5, 5 5)))'), readWKT('MULTILINESTRING ((1 2, 3 4))'), [(1, 2)]::Ring::Geometry",
            "CAST(NULL, 'Geometry'), [[]]::Polygon::Geometry",
            // Values after year 9999: the server writes the bound.
            "fromUnixTimestamp64Second(toInt64(1234567891011), 'UTC'), fromUnixTimestamp64Milli(toInt64(253402300800123), 'UTC'), fromUnixTimestamp64Second(toInt64(253402300799), 'Asia/Makassar'), fromDaysSinceYearZero32(2147483647), reinterpret(toInt64(9223372036854775807), 'DateTime64(3, \\'UTC\\')')",
            "number, toString(number) FROM numbers(3)",
            "toNullable(toUInt64(1)), [NULL, 1], [toNullable('a')], (NULL, 1)",
            // A column or alias named `format` is not the FORMAT clause.
            "1 AS format ORDER BY format DESC",
            "1 AS format ORDER BY format ASC",
            "format FROM (SELECT 1 AS format)",
            "1 AS format FROM numbers(1)",
            "1 FORMAT TSVWithNamesAndTypes",
            "'a\\tb' AS `x y` FORMAT TSVRawWithNames",
    };

    /**
     * Expressions for the client backend only: jdbc-v2 getObject() fails for Interval columns, gives Time and
     * Time64 as java.sql.Time without the sign, the hours over 23 and the fraction, and gives dates before year 1
     * as java.sql.Date / java.sql.Timestamp without the era (known failures in allowlist.txt).
     */
    private static final String[] CLIENT_ONLY_EXPRESSIONS = {
            // Values before year 0: the server writes the bound.
            "fromUnixTimestamp64Milli(toInt64(-62167219200877), 'UTC'), reinterpret(toInt64(-9223372036854775808), 'DateTime64(3, \\'UTC\\')'), fromDaysSinceYearZero32(0) - 1, fromUnixTimestamp64Second(toInt64(-62167219201), 'Asia/Makassar')",
            "INTERVAL 3 DAY, toIntervalSecond(-5), toIntervalMonth(14), toIntervalWeek(2), toIntervalYear(1)",
            "toIntervalNanosecond(5), toIntervalMicrosecond(-3), toIntervalMillisecond(7), toIntervalQuarter(2), toIntervalHour(25)",
            "toTime('12:34:56'), toTime('-1:02:03'), toTime64('12:00:00.5', 3), toTime('100:00:00'), toTime64('00:00:01.1234567', 7)",
    };

    @DataProvider
    public Object[][] expressions() {
        List<Object[]> out = new ArrayList<>();
        for (String backend : BACKENDS) {
            for (String e : EXPRESSIONS) {
                out.add(new Object[] {backend, e});
            }
        }
        for (String e : CLIENT_ONLY_EXPRESSIONS) {
            out.add(new Object[] {"client", e});
        }
        return out.toArray(new Object[0][]);
    }

    /**
     * For each expression, the text that the shim writes from the values that the client decoded must be the same
     * as the server's text.
     */
    @Test(dataProvider = "expressions")
    public void testRenderMatchesServer(String backend, String expr) throws Exception {
        String query = "SELECT " + expr;
        String expected = serverTsv(query);
        ShimResult r = runShim(backend, query);
        assertEquals(r.code, 0, r.stderr);
        assertEquals(r.stdout, expected);
    }

    @DataProvider
    public Object[][] backends() {
        return new Object[][] {{"client"}, {"jdbc"}};
    }

    /** SET and temporary tables last for the whole invocation, also after an error that a test hint expects. */
    @Test(dataProvider = "backends")
    public void testSessionState(String backend) {
        String script = String.join("\n",
                "SET max_threads = 3;",
                "SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }",
                "SELECT getSetting('max_threads');",
                "CREATE TEMPORARY TABLE t (x UInt8);",
                "INSERT INTO t VALUES (1), (2);",
                "INSERT INTO t FORMAT TSV",
                "3",
                "",
                "SELECT sum(x) FROM t;",
                "CREATE DATABASE IF NOT EXISTS clickhouse_client_cli_live;",
                "USE clickhouse_client_cli_live;",
                "SELECT currentDatabase();",
                "DROP DATABASE clickhouse_client_cli_live;");
        ShimResult r = runShim(backend, script);
        assertEquals(r.code, 0, r.stderr);
        assertEquals(r.stdout, "3\n6\nclickhouse_client_cli_live\n");
    }

    @Test(dataProvider = "backends")
    public void testErrors(String backend) {
        // An unexpected error stops the script, writes the error name to stderr, and sets the exit code to the
        // error code.
        ShimResult r = runShim(backend, "SELECT 1; SELECT * FROM no_such_table_xyz; SELECT 2;");
        assertEquals(r.stdout, "1\n");
        assertEquals(r.code, 60);
        assertTrue(r.stderr.contains("UNKNOWN_TABLE"), r.stderr);

        // A query that succeeds although the hint expects an error.
        r = runShim(backend, "SELECT 1; -- { serverError UNKNOWN_TABLE }");
        assertNotEquals(r.code, 0);
        assertTrue(r.stderr.contains("expected serverError UNKNOWN_TABLE"), r.stderr);

        // A different error than the hint expects.
        r = runShim(backend, "SELECT * FROM no_such_table_xyz; -- { serverError SYNTAX_ERROR }");
        assertEquals(r.code, 60, r.stderr);
    }

    /**
     * The settings that tests/clickhouse-test adds for the tags no-async-insert and no-parallel-replicas go with each
     * query, until the test sets them itself.
     */
    @Test(dataProvider = "backends")
    public void testFeatureTagOverrides(String backend) {
        ShimResult r = runShim(backend, String.join("\n",
                "SELECT getSetting('async_insert'), getSetting('enable_parallel_replicas');",
                "SET async_insert = 1;",
                "SELECT getSetting('async_insert');"),
                "--async_insert", "0", "--enable_parallel_replicas", "0");
        assertEquals(r.code, 0, r.stderr);
        assertEquals(r.stdout, "false\t0\ntrue\n");
    }

    /** EXPLAIN ... FORMAT Null prints the plan, except when the session sets ignore_format_null_for_explain = 0. */
    @Test(dataProvider = "backends")
    public void testExplainFormatNull(String backend) throws Exception {
        String plan = serverTsv("EXPLAIN SYNTAX SELECT 1");
        ShimResult r = runShim(backend, String.join("\n",
                "EXPLAIN SYNTAX SELECT 1 FORMAT Null;",
                "SET ignore_format_null_for_explain = 0;",
                "EXPLAIN SYNTAX SELECT 1 FORMAT Null;",
                "SELECT 2 FORMAT Null;",
                "SET ignore_format_null_for_explain = true;",
                "EXPLAIN SYNTAX SELECT 1 FORMAT Null;"));
        assertEquals(r.code, 0, r.stderr);
        assertEquals(r.stdout, plan + plan);
    }

    @DataProvider
    public Object[][] headerFormats() {
        List<Object[]> out = new ArrayList<>();
        for (String backend : BACKENDS) {
            // Contrast: TSVWithNames escapes the header.
            for (String format : new String[] {"TSVWithNames", "TSVRawWithNames", "TabSeparatedRawWithNamesAndTypes"}) {
                out.add(new Object[] {backend, format});
            }
        }
        return out.toArray(new Object[0][]);
    }

    /** The header of a Raw format has no escapes, as its rows. */
    @Test(dataProvider = "headerFormats")
    public void testHeaderOfFormat(String backend, String format) throws Exception {
        String query = "SELECT 1 AS `a\\tb\\\\c'd`, 'x' AS `é\\nf` FORMAT " + format;
        String expected = serverTsv(query);
        ShimResult r = runShim(backend, query);
        assertEquals(r.code, 0, r.stderr);
        assertEquals(r.stdout, expected);
    }

    @Test(dataProvider = "backends")
    public void testEcho(String backend) {
        ShimResult r = runShim(backend, "SELECT 1; -- { echoOn }\nSELECT 2;\nSELECT 3 -- { echoOff }\n;\nSELECT 4\n");
        assertEquals(r.code, 0, r.stderr);
        // The statement with the echoOn hint is echoed, the one with echoOff is not.
        assertEquals(r.stdout, "SELECT 1; -- { echoOn }\n1\nSELECT 2;\n2\n3\n4\n");
    }
}
