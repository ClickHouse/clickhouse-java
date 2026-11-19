package com.clickhouse.client.cli;

import org.apache.commons.cli.ParseException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class CliOptionsTest {

    @Test
    public void testSqlTestCommandLine() throws ParseException {
        // The command line that tests/clickhouse-test uses for a .sql test.
        CliOptions o = CliOptions.parse(new String[] {
                "--host=127.0.0.1", "--port=9000", "--no-secure", "--database=test_abc",
                "--send_logs_level=warning", "--multiquery", "--log_comment", "00001_select_1.sql-test_abc",
                "--max_block_size=1", "--allow_repeated_settings", "--client_logs_file=/tmp/x-fatal",
                "--param_CLICKHOUSE_DATABASE=test_abc"});
        assertEquals(o.host, "127.0.0.1");
        assertEquals(o.port, Integer.valueOf(9000));
        assertEquals(o.database, "test_abc");
        assertFalse(o.secure);
        assertNull(o.query);
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("send_logs_level", "warning");
        settings.put("log_comment", "00001_select_1.sql-test_abc");
        assertEquals(o.settings, settings);
        assertEquals(o.settings.keySet().toArray(), new String[] {"send_logs_level", "log_comment"});
        Map<String, String> params = new HashMap<>();
        params.put("CLICKHOUSE_DATABASE", "test_abc");
        assertEquals(o.params, params);
    }

    @Test
    public void testFeatureTagOverrides() throws ParseException {
        // tests/clickhouse-test adds these for the tags no-parallel-replicas and no-async-insert.
        CliOptions o = CliOptions.parse(new String[] {
                "--database=test_abc", "--multiquery", "--enable_parallel_replicas", "0", "--async_insert", "0",
                "--max_block_size=1"});
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("enable_parallel_replicas", "0");
        settings.put("async_insert", "0");
        assertEquals(o.settings, settings);
    }

    @Test
    public void testShortOptions() throws ParseException {
        CliOptions o = CliOptions.parse(new String[] {"-h", "db", "-u", "me", "-d", "x", "-q", "SELECT 1", "-n", "-s", "-x"});
        assertEquals(o.host, "db");
        assertEquals(o.user, "me");
        assertEquals(o.database, "x");
        assertEquals(o.query, "SELECT 1");
        assertTrue(o.secure);
    }

    @Test
    public void testSettingValues() throws ParseException {
        CliOptions o = CliOptions.parse(new String[] {"--max_threads", "-1", "--max-insert-threads", "2", "--query=SELECT 1"});
        Map<String, String> settings = new HashMap<>();
        settings.put("max_threads", "-1");
        settings.put("max_insert_threads", "2");
        assertEquals(o.settings, settings);
        assertEquals(o.query, "SELECT 1");
    }

    @Test
    public void testErrors() {
        assertThrows(ParseException.class, () -> CliOptions.parse(new String[] {"--port=abc"}));
        assertThrows(ParseException.class, () -> CliOptions.parse(new String[] {"--query"}));
    }

    @Test
    public void testHttpPort() {
        Map<String, String> env = new HashMap<>();
        assertEquals(Main.httpPort(null, false, env::get), 8123);
        assertEquals(Main.httpPort(9000, false, env::get), 8123);
        assertEquals(Main.httpPort(18123, false, env::get), 18123);
        assertEquals(Main.httpPort(9440, true, env::get), 8443);
        env.put("CLICKHOUSE_PORT_TCP", "19000");
        env.put("CLICKHOUSE_PORT_HTTP", "18124");
        assertEquals(Main.httpPort(19000, false, env::get), 18124);
        assertEquals(Main.httpPort(9000, false, env::get), 9000);
    }

    @Test
    public void testExtractFromConfig() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(new ByteArrayOutputStream());
        assertEquals(Main.run(new String[] {"extract-from-config", "--try", "--config", "x.xml", "--key", "listen_host"},
                null, out, err, name -> null), 0);
        assertEquals(new String(out.toByteArray(), StandardCharsets.UTF_8), "127.0.0.1\n");
        out.reset();
        assertEquals(Main.run(new String[] {"extract-from-config", "--key=tcp_port"}, null, out, err, name -> null), 0);
        assertEquals(out.size(), 0);
    }

    @DataProvider
    public Object[][] useStatements() {
        return new Object[][] {
                {"USE db1;", "db1"},
                {"use `my db`", "my db"},
                {"-- c\nUSE DATABASE db2", "db2"},
                {"USE {CLICKHOUSE_DATABASE:Identifier};", "test_x"},
                {"USE \"db3\"", "db3"},
                {"USE db1, db2", null},
        };
    }

    @Test(dataProvider = "useStatements")
    public void testParseUseDatabase(String sql, String expected) {
        Map<String, String> params = new HashMap<>();
        params.put("CLICKHOUSE_DATABASE", "test_x");
        assertEquals(Runner.parseUseDatabase(sql, params), expected);
    }

    @Test
    public void testParseSetAssignments() {
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("dialect", "kusto");
        expected.put("max_threads", "1");
        expected.put("x", "(1, 2)");
        assertEquals(Runner.parseSetAssignments("SET dialect = 'kusto', max_threads=1, x = (1, 2);"), expected);
        assertEquals(Runner.parseSetAssignments("SET a = 'x,y'").keySet().toArray(), Arrays.asList("a").toArray());
    }
}
