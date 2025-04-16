package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QuerySettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import com.clickhouse.data.ClickHouseVersion;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class StatementTest extends JdbcIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(StatementTest.class);

    @Test(groups = { "integration" })
    public void testExecuteQuerySimpleNumbers() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 1 AS num")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getByte(1), 1);
                    assertEquals(rs.getByte("num"), 1);
                    assertEquals(rs.getShort(1), 1);
                    assertEquals(rs.getShort("num"), 1);
                    assertEquals(rs.getInt(1), 1);
                    assertEquals(rs.getInt("num"), 1);
                    assertEquals(rs.getLong(1), 1);
                    assertEquals(rs.getLong("num"), 1);
                    assertFalse(rs.next());
                }
                Assert.assertFalse(((StatementImpl)stmt).getLastQueryId().isEmpty());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteQuerySimpleFloats() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 1.1 AS num")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getFloat(1), 1.1f);
                    assertEquals(rs.getFloat("num"), 1.1f);
                    assertEquals(rs.getDouble(1), 1.1);
                    assertEquals(rs.getDouble("num"), 1.1);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteQueryBooleans() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT true AS flag")) {
                    assertTrue(rs.next());
                    assertTrue(rs.getBoolean(1));
                    assertTrue(rs.getBoolean("flag"));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteQueryStrings() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 'Hello' AS words")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "Hello");
                    assertEquals(rs.getString("words"), "Hello");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteQueryNulls() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT NULL AS nothing")) {
                    assertTrue(rs.next());
                    assertNull(rs.getObject(1));
                    assertNull(rs.getObject("nothing"));
                    assertNull(rs.getString(1));
                    assertNull(rs.getString("nothing"));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteQueryDates() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT toDate('2020-01-01') AS date, toDateTime('2020-01-01 10:11:12', 'Asia/Istanbul') AS datetime")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1).toString(), Date.valueOf("2020-01-01").toString());
                    assertEquals(rs.getDate("date").toString(), Date.valueOf("2020-01-01").toString());
                    assertEquals(rs.getString(1), "2020-01-01");
                    assertEquals(rs.getString("date"), "2020-01-01");
                    assertEquals(rs.getDate(2).toString(), "2020-01-01");
                    assertEquals(rs.getDate("datetime").toString(), "2020-01-01");
                    assertEquals(rs.getString(2), "2020-01-01T10:11:12+03:00[Asia/Istanbul]");
                    assertEquals(rs.getString("datetime"), "2020-01-01T10:11:12+03:00[Asia/Istanbul]");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteUpdateSimpleNumbers() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".simpleNumbers (num UInt8) ENGINE = MergeTree ORDER BY ()"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".simpleNumbers VALUES (1), (2), (3)"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT num FROM " + getDatabase() + ".simpleNumbers ORDER BY num")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 1);
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 2);
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 3);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteUpdateSimpleFloats() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".simpleFloats (num Float32) ENGINE = MergeTree ORDER BY ()"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".simpleFloats VALUES (1.1), (2.2), (3.3)"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT num FROM " + getDatabase() + ".simpleFloats ORDER BY num")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getFloat(1), 1.1f);
                    assertTrue(rs.next());
                    assertEquals(rs.getFloat(1), 2.2f);
                    assertTrue(rs.next());
                    assertEquals(rs.getFloat(1), 3.3f);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteUpdateBooleans() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".booleans (id UInt8, flag Boolean) ENGINE = MergeTree ORDER BY ()"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".booleans VALUES (0, true), (1, false), (2, true)"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT flag FROM " + getDatabase() + ".booleans ORDER BY id")) {
                    assertTrue(rs.next());
                    assertTrue(rs.getBoolean(1));
                    assertTrue(rs.next());
                    assertFalse(rs.getBoolean(1));
                    assertTrue(rs.next());
                    assertTrue(rs.getBoolean(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteUpdateStrings() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".strings (id UInt8, words String) ENGINE = MergeTree ORDER BY ()"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".strings VALUES (0, 'Hello'), (1, 'World'), (2, 'ClickHouse')"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT words FROM " + getDatabase() + ".strings ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "Hello");
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "World");
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "ClickHouse");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteUpdateNulls() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".nulls (id UInt8, nothing Nullable(String)) ENGINE = MergeTree ORDER BY ()"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".nulls VALUES (0, 'Hello'), (1, NULL), (2, 'ClickHouse')"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT nothing FROM " + getDatabase() + ".nulls ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "Hello");
                    assertTrue(rs.next());
                    assertNull(rs.getString(1));
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "ClickHouse");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteUpdateDates() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".dates (id UInt8, date Nullable(Date), datetime Nullable(DateTime)) ENGINE = MergeTree ORDER BY ()"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".dates VALUES (0, '2020-01-01', '2020-01-01 10:11:12'), (1, NULL, '2020-01-01 12:10:07'), (2, '2020-01-01', NULL)"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT date, datetime FROM " + getDatabase() + ".dates ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1).toString(), "2020-01-01");
                    assertEquals(rs.getDate(2).toString(), "2020-01-01");
                    assertTrue(rs.next());
                    assertNull(rs.getDate(1));
                    assertEquals(rs.getDate(2).toString(), "2020-01-01");
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1).toString(), "2020-01-01");
                    assertNull(rs.getDate(2));
                    assertFalse(rs.next());
                }
            }
        }
    }


    @Test(groups = { "integration" })
    public void testExecuteUpdateBatch() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".batch (id UInt8, num UInt8) ENGINE = MergeTree ORDER BY ()"), 0);
                stmt.addBatch("INSERT INTO " + getDatabase() + ".batch VALUES (0, 1)");
                stmt.addBatch("INSERT INTO " + getDatabase() + ".batch VALUES (1, 2)");
                stmt.addBatch("INSERT INTO " + getDatabase() + ".batch VALUES (2, 3), (3, 4)");
                int[] counts = stmt.executeBatch();
                assertEquals(counts.length, 3);
                assertEquals(counts[0], 1);
                assertEquals(counts[1], 1);
                assertEquals(counts[2], 2);
                try (ResultSet rs = stmt.executeQuery("SELECT num FROM " + getDatabase() + ".batch ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 1);
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 2);
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 3);
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 4);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testJdbcEscapeSyntax() throws Exception {
        if (ClickHouseVersion.of(getServerVersion()).check("(,23.8]")) {
            return; // there is no `timestamp` function TODO: fix in JDBC
        }
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT {d '2021-11-01'} AS D, {ts '2021-08-01 12:34:56'} AS TS, " +
                        "toInt32({fn ABS(-1)}) AS FNABS, {fn CONCAT('Hello', 'World')} AS FNCONCAT, {fn UCASE('hello')} AS FNUPPER, " +
                        "{fn LCASE('HELLO')} AS FNLOWER, {fn LTRIM('  Hello  ')} AS FNLTRIM, {fn RTRIM('  Hello  ')} AS FNRTRIM, " +
                        "toInt32({fn LENGTH('Hello')}) AS FNLENGTH, toInt32({fn POSITION('Hello', 'l')}) AS FNPOSITION, toInt32({fn MOD(10, 3)}) AS FNMOD, " +
                        "{fn SQRT(9)} AS FNSQRT, {fn SUBSTRING('Hello', 3, 2)} AS FNSUBSTRING")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1), Date.valueOf(LocalDate.of(2021, 11, 1)));
                    //assertEquals(rs.getTimestamp(2), java.sql.Timestamp.valueOf(LocalDateTime.of(2021, 11, 1, 12, 34, 56)));
                    assertEquals(rs.getInt(3), 1);
                    assertEquals(rs.getInt("FNABS"), 1);
                    assertEquals(rs.getString(4), "HelloWorld");
                    assertEquals(rs.getString("FNCONCAT"), "HelloWorld");
                    assertEquals(rs.getString(5), "HELLO");
                    assertEquals(rs.getString("FNUPPER"), "HELLO");
                    assertEquals(rs.getString(6), "hello");
                    assertEquals(rs.getString("FNLOWER"), "hello");
                    assertEquals(rs.getString(7), "Hello  ");
                    assertEquals(rs.getString("FNLTRIM"), "Hello  ");
                    assertEquals(rs.getString(8), "  Hello");
                    assertEquals(rs.getString("FNRTRIM"), "  Hello");
                    assertEquals(rs.getInt(9), 5);
                    assertEquals(rs.getInt("FNLENGTH"), 5);
                    assertEquals(rs.getInt(10), 3);
                    assertEquals(rs.getInt("FNPOSITION"), 3);
                    assertEquals(rs.getInt(11), 1);
                    assertEquals(rs.getInt("FNMOD"), 1);
                    assertEquals(rs.getDouble(12), 3);
                    assertEquals(rs.getDouble("FNSQRT"), 3);
                    assertEquals(rs.getString(13), "ll");
                    assertEquals(rs.getString("FNSUBSTRING"), "ll");
                    assertThrows(SQLException.class, () -> rs.getString(14));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteQueryTimeout() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(1);
                assertThrows(SQLException.class, () -> {
                    try (ResultSet rs = stmt.executeQuery("SELECT sleep(5)")) {
                        assertFalse(rs.next());
                    }
                });
            }
        }
    }


    @Test(groups = { "integration" })
    private void testSettingRole() throws SQLException {
        if (earlierThan(24, 4)) {//Min version is 24.4
            return;
        }

        List<String> roles = Arrays.asList("role1", "role2", "role3");

        String userPass = "^1A" + RandomStringUtils.random(12, true, true) + "3B$";
        try (ConnectionImpl conn = (ConnectionImpl) getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP ROLE IF EXISTS " + String.join(", ", roles));
                stmt.execute("DROP USER IF EXISTS some_user");
                stmt.execute("CREATE ROLE " + String.join(", ", roles));
                stmt.execute("CREATE USER some_user IDENTIFIED BY '" + userPass + "'");
                stmt.execute("GRANT " + String.join(", ", roles) + " TO some_user");
                stmt.execute("SET DEFAULT ROLE NONE TO some_user");
            }
        }

        Properties info = new Properties();
        info.setProperty("user", "some_user");
        info.setProperty("password", userPass);

        try (ConnectionImpl conn = new ConnectionImpl(getEndpointString(), info)) {
            GenericRecord record = conn.client.queryAll("SELECT currentRoles()").get(0);
            assertEquals(record.getList(1).size(), 0);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET ROLE role1");
            }

            record = conn.client.queryAll("SELECT currentRoles()").get(0);
            assertEquals(record.getList(1).size(), 1);
            assertEquals(record.getList(1).get(0), "role1");

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET ROLE role2");
            }

            record = conn.client.queryAll("SELECT currentRoles()").get(0);
            assertEquals(record.getList(1).size(), 1);
            assertEquals(record.getList(1).get(0), "role2");

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET ROLE NONE");
            }

            record = conn.client.queryAll("SELECT currentRoles()").get(0);
            assertEquals(record.getList(1).size(), 0);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET ROLE \"role1\",\"role2\"");
            }

            record = conn.client.queryAll("SELECT currentRoles()").get(0);
            assertEquals(record.getList(1).size(), 2);
            assertEquals(record.getList(1).get(0), "role1");
            assertEquals(record.getList(1).get(1), "role2");

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET ROLE \"role1\",\"role2\",\"role3\"");
            }

            record = conn.client.queryAll("SELECT currentRoles()").get(0);
            assertEquals(record.getList(1).size(), 3);
            assertEquals(record.getList(1).get(0), "role1");
            assertEquals(record.getList(1).get(1), "role2");
            assertEquals(record.getList(1).get(2), "role3");
        }
    }

    @Test
    public void testGettingArrays() throws Exception {
        try (ConnectionImpl conn = (ConnectionImpl) getJdbcConnection();
             Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery("SELECT [] as empty_array, [1, 2, 3] as number_array, " +
                    " ['val1', 'val2', 'val3'] as str_array");

            assertTrue(rs.next());
            Array emptyArray = rs.getArray("empty_array");
            assertEquals(((Object[]) emptyArray.getArray()).length, 0);
            Array numberArray = rs.getArray("number_array");
            assertEquals(((Object[]) numberArray.getArray()).length, 3);
            System.out.println(((Object[]) numberArray.getArray())[0].getClass().getName());
            assertEquals(numberArray.getArray(), new short[] {1, 2, 3} );
            Array stringArray = rs.getArray("str_array");
            assertEquals(((Object[]) stringArray.getArray()).length, 3);
            assertEquals(Arrays.stream(((Object[]) stringArray.getArray())).toList(), Arrays.asList("val1", "val2", "val3"));
        }
    }

    @Test(groups = { "integration" })
    public void testWithComments() throws Exception {
        assertEquals(StatementImpl.parseStatementType("    /* INSERT TESTING */\n SELECT 1 AS num"), StatementImpl.StatementType.SELECT);
        assertEquals(StatementImpl.parseStatementType("/* SELECT TESTING */\n INSERT INTO test_table VALUES (1)"), StatementImpl.StatementType.INSERT);
        assertEquals(StatementImpl.parseStatementType("/* INSERT TESTING */\n\n\n UPDATE test_table SET num = 2"), StatementImpl.StatementType.UPDATE);
        assertEquals(StatementImpl.parseStatementType("-- INSERT TESTING */\n SELECT 1 AS num"), StatementImpl.StatementType.SELECT);
        assertEquals(StatementImpl.parseStatementType("     -- SELECT TESTING \n -- SELECT AGAIN \n INSERT INTO test_table VALUES (1)"), StatementImpl.StatementType.INSERT);
        assertEquals(StatementImpl.parseStatementType(" SELECT 42    -- INSERT TESTING"), StatementImpl.StatementType.SELECT);
        assertEquals(StatementImpl.parseStatementType("#! INSERT TESTING \n SELECT 1 AS num"), StatementImpl.StatementType.SELECT);
        assertEquals(StatementImpl.parseStatementType("#!INSERT TESTING \n SELECT 1 AS num"), StatementImpl.StatementType.SELECT);
        assertEquals(StatementImpl.parseStatementType("# INSERT TESTING \n SELECT 1 AS num"), StatementImpl.StatementType.SELECT);
        assertEquals(StatementImpl.parseStatementType("#INSERT TESTING \n SELECT 1 AS num"), StatementImpl.StatementType.SELECT);
        assertEquals(StatementImpl.parseStatementType("\nINSERT TESTING \n SELECT 1 AS num"), StatementImpl.StatementType.INSERT);
        assertEquals(StatementImpl.parseStatementType("         \n          INSERT TESTING \n SELECT 1 AS num"), StatementImpl.StatementType.INSERT);
        assertEquals(StatementImpl.parseStatementType("select 1 AS num"), StatementImpl.StatementType.SELECT);
        assertEquals(StatementImpl.parseStatementType("insert into test_table values (1)"), StatementImpl.StatementType.INSERT);
        assertEquals(StatementImpl.parseStatementType("update test_table set num = 2"), StatementImpl.StatementType.UPDATE);
        assertEquals(StatementImpl.parseStatementType("delete from test_table where num = 2"), StatementImpl.StatementType.DELETE);
        assertEquals(StatementImpl.parseStatementType("sElEcT 1 AS num"), StatementImpl.StatementType.SELECT);
        assertEquals(StatementImpl.parseStatementType(null), StatementImpl.StatementType.OTHER);
        assertEquals(StatementImpl.parseStatementType(""), StatementImpl.StatementType.OTHER);
        assertEquals(StatementImpl.parseStatementType("      "), StatementImpl.StatementType.OTHER);
    }

    @Test(groups = { "integration" })
    public void testParseStatementWithClause() throws Exception {
        assertEquals(StatementImpl.parseStatementType("with data as (SELECT number FROM numbers(100)) select * from data"), StatementImpl.StatementType.SELECT);
    }


    @Test(groups = { "integration" })
    public void testWithIPs() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT toIPv4('127.0.0.1'), toIPv6('::1'), toIPv6('2001:438:ffff::407d:1bc1')")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "/127.0.0.1");
                    assertEquals(rs.getObject(1), Inet4Address.getByName("127.0.0.1"));
                    assertEquals(rs.getString(2), "/0:0:0:0:0:0:0:1");
                    assertEquals(rs.getObject(2), Inet6Address.getByName("0:0:0:0:0:0:0:1"));
                    assertEquals(rs.getString(3), "/2001:438:ffff:0:0:0:407d:1bc1");
                    assertEquals(rs.getObject(3), Inet6Address.getByName("2001:438:ffff:0:0:0:407d:1bc1"));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testConnectionExhaustion() throws Exception {

        int maxNumConnections = 3;
        Properties properties = new Properties();
        properties.put(ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS.getKey(), "" + maxNumConnections);
        properties.put(ClientConfigProperties.CONNECTION_REQUEST_TIMEOUT.getKey(), "" + 1000); // 1 sec connection req timeout

        try (Connection conn = getJdbcConnection(properties)) {
            try (Statement stmt = conn.createStatement()) {
                for (int i = 0; i< maxNumConnections * 2; i++) {
                    stmt.executeQuery("SELECT number FROM system.numbers LIMIT 100");
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testConcurrentCancel() throws Exception {
        int maxNumConnections = 3;
        Properties p = new Properties();
        p.put(ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS.getKey(), String.valueOf(maxNumConnections));
        try (Connection conn = getJdbcConnection()) {
            try (StatementImpl stmt = (StatementImpl) conn.createStatement()) {
                stmt.executeQuery("SELECT number FROM system.numbers LIMIT 1000000");
                stmt.cancel();
            }
            for (int i = 0; i < maxNumConnections; i++) {
                try (StatementImpl stmt = (StatementImpl) conn.createStatement()) {
                    final int threadNum = i;
                    log.info("Starting thread {}", threadNum);
                    final CountDownLatch latch = new CountDownLatch(1);
                    Thread t = new Thread(() -> {
                        try {
                            latch.countDown();
                            ResultSet rs = stmt.executeQuery("SELECT number FROM system.numbers LIMIT 10000000");
                        } catch (SQLException e) {
                            log.error("Error in thread {}", threadNum, e);
                        }
                    });
                    t.start();

                    latch.await();
                    stmt.cancel();
                }
            }
        }
    }

    @Test(groups = {"integration"})
    public void testTextFormatInResponse() throws Exception {
        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement()) {
            Assert.expectThrows(SQLException.class, () ->stmt.executeQuery("SELECT 1 FORMAT JSON"));
        }
    }

    @Test(groups = "integration")
    void testWithClause() throws Exception {
        int count = 0;
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("with data as (SELECT number FROM numbers(100)) select * from data");
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    count++;
                }
            }
        }
        assertEquals(count, 100);
    }

    @Test(groups = { "integration" })
    public void testSwitchDatabase() throws Exception {
        String databaseName = getDatabase() + "_test_switch";
        String createSql = "CREATE TABLE switchDatabaseWithUse (id UInt8, words String) ENGINE = MergeTree ORDER BY ()";
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate(createSql), 0);
                assertEquals(stmt.executeUpdate("CREATE DATABASE \"" + databaseName + "\""), 0);
                assertFalse(stmt.execute("USE \"" + databaseName + "\""));
                assertEquals(stmt.executeUpdate(createSql), 0);
            }
        }
    }
  
  
    @Test(groups = { "integration" })
    public void testNewLineSQLParsing() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            String sqlCreate = "CREATE TABLE balance ( `id` UUID, `currency` String, `amount` Decimal(64, 18), `create_time` DateTime64(6), `_version` UInt64, `_sign` UInt8 ) ENGINE = ReplacingMergeTree PRIMARY KEY id ORDER BY id;";
            try (Statement stmt = conn.createStatement()) {
                int r = stmt.executeUpdate(sqlCreate);
                assertEquals(r, 0);
            }
            try (Statement stmt = conn.createStatement()) {
                String sqlInsert = "INSERT INTO balance VALUES (generateUUIDv4(), 'EUR', '42.42', now(), 144, 255);";
                int r = stmt.executeUpdate(sqlInsert);
                assertEquals(r, 1);
            }
            try (Statement stmt = conn.createStatement()) {
                String sqlSelect = new StringBuilder("-- SELECT amount FROM balance FINAL;\n")
                        .append("SELECT amount FROM balance FINAL;").toString();
                ResultSet rs = stmt.executeQuery(sqlSelect);
                assertTrue(rs.next());
            }
            try (Statement stmt = conn.createStatement()) {
                String sqlSelect = new StringBuilder("-- SELECT * FROM balance\n")
                        .append("\n")
                        .append("WITH balance_cte AS (\n")
                        .append("SELECT\n")
                        .append("id, currency, amount\n")
                        .append("FROM balance\n")
                        .append("LIMIT 10\n")
                        .append(")\n")
                        .append("SELECT * FROM balance_cte;").toString();
                ResultSet rs = stmt.executeQuery(sqlSelect);
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
            try (Statement stmt = conn.createStatement()) {
                String sqlSelect = new StringBuilder("-- SELECT amount FROM balance FINAL;\n")
                        .append("\n")
                        .append("SELECT amount FROM balance FINAL;").toString();
                ResultSet rs = stmt.executeQuery(sqlSelect);
                assertTrue(rs.next());
            }
            try (Statement stmt = conn.createStatement()) {
                String sqlSelect = new StringBuilder("-- SELECT amount FROM balance FINAL;\n")
                        .append("\n")
                        .append("SELECT amount /* test */FROM balance FINAL;").toString();
                ResultSet rs = stmt.executeQuery(sqlSelect);
                assertTrue(rs.next());
            }
            try (Statement stmt = conn.createStatement()) {
                String sqlSelect = new StringBuilder("-- SELECT amount FROM balance FINAL;\n")
                        .append("\n")
                        .append("SELECT amount FROM balance FINAL; /* test */").toString();
                ResultSet rs = stmt.executeQuery(sqlSelect);
                assertTrue(rs.next());
            }
            try (Statement stmt = conn.createStatement()) {
                String sqlSelect = new StringBuilder("-- SELECT amount FROM balance FINAL;\n")
                        .append("\n")
                        .append("SELECT amount FROM balance FINAL; /* test */ -- SELECT 1").toString();
                ResultSet rs = stmt.executeQuery(sqlSelect);
                assertTrue(rs.next());
            }
        }
    }

    
    @Test(groups = { "integration" })
    public void testNullableFixedStringType() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            String sqlCreate = "CREATE TABLE `data_types` (`f1` FixedString(4),`f2` LowCardinality(FixedString(4)), `f3` Nullable(FixedString(4)), `f4` LowCardinality(Nullable(FixedString(4))) ) ENGINE Memory;";
            try (Statement stmt = conn.createStatement()) {
                int r = stmt.executeUpdate(sqlCreate);
                assertEquals(r, 0);
            }
            try(Statement stmt = conn.createStatement()) {
                String sqlInsert = "INSERT INTO `data_types` VALUES ('val1', 'val2', 'val3', 'val4')";
                int r = stmt.executeUpdate(sqlInsert);
                assertEquals(r, 1);
            }
            try(Statement stmt = conn.createStatement()) {
                String sqlSelect = "SELECT * FROM `data_types`";
                ResultSet rs = stmt.executeQuery(sqlSelect);
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "val1");
                assertEquals(rs.getString(2), "val2");
                assertEquals(rs.getString(3), "val3");
                assertEquals(rs.getString(4), "val4");
                assertFalse(rs.next());
            }
            try(Statement stmt = conn.createStatement()) {
                String sqlSelect = "SELECT f4 FROM `data_types`";
                ResultSet rs = stmt.executeQuery(sqlSelect);
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "val4");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testWasNullFlagArray() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            String sql = "SELECT NULL, ['value1', 'value2']";
            Statement stmt = conn.createStatement();
            stmt.executeQuery(sql);
            ResultSet rs = stmt.getResultSet();
            assertTrue(rs.next());
            int val = rs.getInt(1);
            assertTrue(rs.wasNull());
            Array arr = rs.getArray(2);
            assertFalse(rs.wasNull());
            assertNotNull(arr);
            Object[] values = (Object[]) arr.getArray();
            assertNotNull(values);
            assertEquals(values.length, 2);
            assertEquals(values[0], "value1");
            assertEquals(values[1], "value2");
        }

        try (Connection conn = getJdbcConnection()) {
            String sql = "SELECT NULL, ['value1', 'value2'] AS array";
            Statement stmt = conn.createStatement();
            stmt.executeQuery(sql);
            ResultSet rs = stmt.getResultSet();
            assertTrue(rs.next());
            int val = rs.getInt(1);
            assertTrue(rs.wasNull());
            Array arr = rs.getArray("array");
            assertFalse(rs.wasNull());
            assertNotNull(arr);
            Object[] values = (Object[]) arr.getArray();
            assertNotNull(values);
            assertEquals(values.length, 2);
            assertEquals(values[0], "value1");
            assertEquals(values[1], "value2");
        }
    }

    @Test(groups = { "integration" })
    public void testExecuteWithMaxRows() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.setMaxRows(1);
                int count = 0;
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM generate_series(0, 100000)")) {
                    while (rs.next()) {
                        count++;
                    }
                }
                // MaxRows limits the number of row with rounding to the size of the block
                // https://clickhouse.com/docs/en/operations/settings/query-complexity#setting-max_result_rows
                // https://clickhouse.com/docs/en/operations/settings/query-complexity#result-overflow-mode
                assertTrue(count > 0 && count < 100000);
            }
        }
    }

}
