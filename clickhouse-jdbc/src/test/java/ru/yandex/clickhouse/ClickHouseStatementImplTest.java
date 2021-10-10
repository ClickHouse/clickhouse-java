package ru.yandex.clickhouse;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.clickhouse.client.ClickHouseVersion;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

public class ClickHouseStatementImplTest extends JdbcIntegrationTest {
    private ClickHouseConnection connection;

    @BeforeClass(groups = "integration")
    public void setUp() throws Exception {
        connection = newConnection();
    }

    @AfterClass(groups = "integration")
    public void tearDown() throws Exception {
        closeConnection(connection);
    }

    @Test(groups = "integration")
    public void testUpdateCountForSelect() throws Exception {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT dummy FROM system.one");
        Assert.assertEquals(stmt.getUpdateCount(), -1);
        rs.next();
        Assert.assertEquals(stmt.getUpdateCount(), -1);
        rs.close();
        stmt.close();
    }

    @Test(groups = "integration")
    public void testSingleColumnResultSet() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("select c from (\n" +
                "    select 'a' as c, 1 as rn\n" +
                "    UNION ALL select 'b' as c, 2 as rn\n" +
                "    UNION ALL select '' as c, 3 as rn\n" +
                "    UNION ALL select 'd' as c, 4 as rn\n" +
                " ) order by rn");
        StringBuffer sb = new StringBuffer();
        while (rs.next()) {
            sb.append(rs.getString("c")).append("\n");
        }
        Assert.assertEquals(sb.toString(), "a\nb\n\nd\n");
    }

    @Test(groups = "integration")
    public void readsPastLastAreSafe() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("select c from (\n" +
                "    select 'a' as c, 1 as rn\n" +
                "    UNION ALL select 'b' as c, 2 as rn\n" +
                "    UNION ALL select '' as c, 3 as rn\n" +
                "    UNION ALL select 'd' as c, 4 as rn\n" +
                " ) order by rn");
        StringBuffer sb = new StringBuffer();
        while (rs.next()) {
            sb.append(rs.getString("c")).append("\n");
        }
        Assert.assertFalse(rs.next());
        Assert.assertFalse(rs.next());
        Assert.assertFalse(rs.next());
        Assert.assertEquals(sb.toString(), "a\nb\n\nd\n");
    }

    @Test(groups = "integration")
    public void testSelectUInt32() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select toUInt32(10), toUInt32(-10)");
        rs.next();
        Object smallUInt32 = rs.getObject(1);
        Assert.assertTrue(smallUInt32 instanceof Long);
        Assert.assertEquals(((Long)smallUInt32).longValue(), 10);
        Object bigUInt32 = rs.getObject(2);
        Assert.assertTrue(bigUInt32 instanceof Long);
        Assert.assertEquals(((Long)bigUInt32).longValue(), 4294967286L);
    }

    @Test(groups = "integration")
    public void testSelectUInt64() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select toUInt64(10), toUInt64(-10)");
        rs.next();
        Object smallUInt64 = rs.getObject(1);
        Assert.assertTrue(smallUInt64 instanceof BigInteger);
        Assert.assertEquals(((BigInteger)smallUInt64).intValue(), 10);
        Object bigUInt64 = rs.getObject(2);
        Assert.assertTrue(bigUInt64 instanceof BigInteger);
        Assert.assertEquals(bigUInt64, new BigInteger("18446744073709551606"));
    }

    @Test(groups = "integration")
    public void testExternalData() throws SQLException, UnsupportedEncodingException {
        ClickHouseStatement stmt = connection.createStatement();
        String[] rows = ClickHouseVersion.check(connection.getServerVersion(), "[21.3,)")
            ? new String[] { "1\tGroup\n" }
            : new String[] { "1\tGroup", "1\tGroup\n" };
        
        for (String row : rows) {
            try (ResultSet rs = stmt.executeQuery(
                "select UserName, GroupName " +
                "from (select 'User' as UserName, 1 as GroupId) as g" +
                "any left join groups using GroupId", null,
                Collections.singletonList(new ClickHouseExternalData(
                        "groups",
                        new ByteArrayInputStream(row.getBytes())
                ).withStructure("GroupId UInt8, GroupName String")))) {
                Assert.assertTrue(rs.next());
                String userName = rs.getString("UserName");
                String groupName = rs.getString("GroupName");

                Assert.assertEquals(userName, "User");
                Assert.assertEquals(groupName, "Group");
            }
        }
    }

    // reproduce issue #634
    @Test(groups = "integration")
    public void testLargeQueryWithExternalData() throws Exception {
        String[] rows = ClickHouseVersion.check(connection.getServerVersion(), "[21.3)")
            ? new String[] { "1\tGroup\n" }
            : new String[] { "1\tGroup", "1\tGroup\n" };
        
        int length = 160000;
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append('u');
        }
        String user = builder.toString();
        for (String row : rows) {
            try (ClickHouseStatement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                    "select UserName, GroupName from (select '"
                    + user
                    + "' as UserName, 1 as GroupId) as g"
                    + "any left join groups using GroupId", null,
                    Collections.singletonList(new ClickHouseExternalData(
                        "groups", new ByteArrayInputStream(row.getBytes())
                ).withStructure("GroupId UInt8, GroupName String")))) {
                Assert.assertTrue(rs.next());
                String userName = rs.getString("UserName");
                String groupName = rs.getString("GroupName");

                Assert.assertEquals(userName, user);
                Assert.assertEquals(groupName, "Group");
            }
        }
    }


    private InputStream getTSVStream(final int rowsCount) {
        return new InputStream() {
            private int si = 0;
            private String s = "";
            private int i = 0;

            private boolean genNextString() {
                if (i >= rowsCount) {
                    return false;
                }
                si = 0;
                s = String.format("%d\t%d\n", i, i);
                i++;
                return true;
            }

            @Override
            public int read() {
                if (si >= s.length()) {
                    if (!genNextString()) {
                        return -1;
                    }
                }
                return s.charAt(si++);
            }
        };
    }


    @Test(groups = "integration")
    public void testExternalDataStream() throws SQLException, UnsupportedEncodingException {
        if ("21.3.3.14".equals(connection.getServerVersion())) {
            return;
        }

        final ClickHouseStatement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS testExternalData");
        statement.execute(
            "CREATE TABLE testExternalData (id UInt64, s String) ENGINE = Memory");
        statement.execute(
            "insert into testExternalData select number, toString(number) from numbers(500,100000)");

        InputStream inputStream = getTSVStream(100000);

        ClickHouseExternalData extData = new ClickHouseExternalData("ext_data", inputStream);
        extData.setStructure("id UInt64, s String");

        ResultSet rs = statement.executeQuery(
                "select count() cnt from testExternalData where (id,s) in ext_data",
                null,
                Collections.singletonList(extData)
        );

        rs.next();

        int cnt = rs.getInt("cnt");

        Assert.assertEquals(cnt, 99500);
    }

    @Test(groups = "integration")
    public void testQueryWithMultipleExternalTables() throws SQLException {
        int tables = 30;
        int rows = 10;

        String ddl = "drop table if exists test_ext_data_query;\n"
            + "create table test_ext_data_query (\n"
            + "   Cb String,\n"
            + "   CREATETIME DateTime64(3),\n"
            + "   TIMESTAMP UInt64,\n"
            + "   Cc String,\n"
            + "   Ca1 UInt64,\n"
            + "   Ca2 UInt64,\n"
            + "   Ca3 UInt64\n"
            + ") engine = MergeTree()\n"
            + "PARTITION BY toYYYYMMDD(CREATETIME)\n"
            + "ORDER BY (Cb, CREATETIME, Cc);";

        String template = "avgIf(Ca1, Cb in L%1$d) as avgCa1%2$d, sumIf(Ca1, Cb in L%1$d) as sumCa1%2$d, minIf(Ca1, Cb in L%1$d) as minCa1%2$d, maxIf(Ca1, Cb in L%1$d) as maxCa1%2$d, anyIf(Ca1, Cb in L%1$d) as anyCa1%2$d, avgIf(Ca2, Cb in L%1$d) as avgCa2%2$d, sumIf(Ca2, Cb in L%1$d) as sumCa2%2$d, minIf(Ca2, Cb in L%1$d) as minCa2%2$d, maxIf(Ca2, Cb in L%1$d) as maxCa2%2$d, anyIf(Ca2, Cb in L%1$d) as anyCa2%2$d, avgIf(Ca3, Cb in L%1$d) as avgCa3%2$d, sumIf(Ca3, Cb in L%1$d) as sumCa3%2$d, minIf(Ca3, Cb in L%1$d) as minCa3%2$d, maxIf(Ca3, Cb in L%1$d) as maxCa3%2$d, anyIf(Ca3, Cb in L%1$d) as anyCa3%2$d";
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setDatabase(dbName);
        properties.setSocketTimeout(300000);
        properties.setMaxAstElements(Long.MAX_VALUE);
        properties.setMaxTotal(20);
        properties.setMaxQuerySize(104857600L);

        Map<ClickHouseQueryParam, String> paramMap = new HashMap<>();
        paramMap.put(ClickHouseQueryParam.PRIORITY,"2");

        StringBuilder sql = new StringBuilder().append("select ");
        List<ClickHouseExternalData> extDataList = new ArrayList<>(tables);
        for (int i = 0; i < tables; i++) {
            sql.append(String.format(template, i, i + 1)).append(',');
            List<String> valueList = new ArrayList<>(rows);
            for (int j = i, size = i + rows; j < size; j++) {
                valueList.add(String.valueOf(j));
            }
            String dnExtString = String.join("\n", valueList);
            InputStream inputStream = new ByteArrayInputStream(dnExtString.getBytes(Charset.forName("UTF-8")));
            ClickHouseExternalData extData = new ClickHouseExternalData("L" + i, inputStream);
            extData.setStructure("Cb String");
            extDataList.add(extData);
        }

        if (tables > 0) {
            sql.deleteCharAt(sql.length() - 1);
        } else {
            sql.append('*');
        }
        sql.append(" from test_ext_data_query where TIMESTAMP >= 1625796480 and TIMESTAMP < 1625796540 and Cc = 'eth0'");

        try (ClickHouseConnection c = newDataSource(properties).getConnection();
            ClickHouseStatement s = c.createStatement();) {
            s.execute(ddl);
            ResultSet rs = s.executeQuery(sql.toString(), paramMap, extDataList);
            assertTrue(tables <= 0 || rs.next());
        }
    }

    @Test(groups = "integration")
    public void testResultSetWithExtremes() throws SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setExtremes(true);
        ClickHouseDataSource dataSource = newDataSource(properties);
        Connection connection = dataSource.getConnection();

        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select 1");
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1)).append("\n");
            }

            Assert.assertEquals(sb.toString(), "1\n");
        } finally {
            connection.close();
        }
    }

    @Test(groups = "integration")
    public void testSelectOne() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("select\n1");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testSelectManyRows() throws SQLException {
        Statement stmt = connection.createStatement();
        int limit = 10000;
        ResultSet rs = stmt.executeQuery(
            "select concat('test', toString(number)) as str from system.numbers limit " + limit);
        int i = 0;
        while (rs.next()) {
            String s = rs.getString("str");
            Assert.assertEquals(s, "test" + i);
            i++;
        }
        Assert.assertEquals(i, limit);
    }

    @Test(groups = "integration")
    public void testMoreResultsWithResultSet() throws SQLException {
        Statement stmt = connection.createStatement();

        Assert.assertTrue(stmt.execute("select 1"));
        Assert.assertNotNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), -1);

        Assert.assertFalse(stmt.getMoreResults());
        Assert.assertNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), -1);
    }

    @Test(groups = "integration")
    public void testMoreResultsWithUpdateCount() throws SQLException {
        Statement stmt = connection.createStatement();

        Assert.assertFalse(stmt.execute("create database if not exists default"));
        Assert.assertNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), 0);

        Assert.assertFalse(stmt.getMoreResults());
        Assert.assertNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), -1);
    }

    @Test(groups = "integration")
    public void testSelectQueryStartingWithWith() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("WITH 2 AS two SELECT two * two;");

        Assert.assertNotNull(rs);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt(1), 4);

        rs.close();
        stmt.close();
    }

    @Test(groups = "integration")
    public void cancelTest_queryId_is_not_set() throws Exception {
        final ClickHouseStatement firstStatement = newConnection().createStatement();

        final AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    Map<ClickHouseQueryParam, String> params = new EnumMap<>(ClickHouseQueryParam.class);
                    params.put(ClickHouseQueryParam.CONNECT_TIMEOUT, Long.toString(TimeUnit.MINUTES.toMillis(1)));
                    firstStatement.executeQuery("SELECT count() FROM system.numbers", params);
                } catch (Exception e) {
                    exceptionAtomicReference.set(e);
                }
            }
        };
        thread.setDaemon(true);
        thread.start();


        final long timeout = 10;
        assertTrue(firstStatement instanceof ClickHouseStatementImpl);
        String queryId = readQueryId((ClickHouseStatementImpl) firstStatement, timeout);
        assertNotNull(
            String.format("it's actually very strange. It seems the query hasn't been executed in %s seconds", timeout),
            queryId);
        assertNull(exceptionAtomicReference.get(), "An exception happened while the query was being executed");


        assertTrue(checkQuery(queryId, true, 10), "The query isn't being executed. It seems very strange");
        firstStatement.cancel();
        assertTrue(checkQuery(queryId, false, 10), "The query is still being executed");

        firstStatement.close();
        thread.interrupt();
    }


    @Test(groups = "integration")
    public void cancelTest_queryId_is_set() throws Exception {
        final String queryId = UUID.randomUUID().toString();
        final ClickHouseStatement firstStatement = newConnection().createStatement();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    Map<ClickHouseQueryParam, String> params = new EnumMap<>(ClickHouseQueryParam.class);
                    params.put(ClickHouseQueryParam.CONNECT_TIMEOUT, Long.toString(TimeUnit.MINUTES.toMillis(1)));
                    params.put(ClickHouseQueryParam.QUERY_ID, queryId);
                    countDownLatch.countDown();
                    firstStatement.executeQuery("SELECT count() FROM system.numbers", params);
                } catch (Exception e) {
                    exceptionAtomicReference.set(e);
                }
            }
        };
        thread.setDaemon(true);
        thread.start();
        final long timeout = 10;
        assertTrue(countDownLatch.await(timeout, TimeUnit.SECONDS),
            String.format(
                "it's actually very strange. It seems the query hasn't been executed in %s seconds", timeout));
        assertNull(exceptionAtomicReference.get(), "An exception happened while the query was being executed");

        assertTrue(checkQuery(queryId, true, 10), "The query isn't being executed. It seems very strange");
        firstStatement.cancel();
        assertTrue(checkQuery(queryId, false, 10), "The query is still being executed");

        firstStatement.close();
        thread.interrupt();
    }

    @Test(groups = "integration")
    public void testArrayMetaActualExecutiom() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT array(42, 23)");
        rs.next();
        Assert.assertEquals(rs.getMetaData().getColumnType(1), Types.ARRAY);
        Assert.assertEquals(rs.getMetaData().getColumnTypeName(1), "Array(UInt8)");
        Assert.assertEquals(rs.getMetaData().getColumnClassName(1),
            Array.class.getCanonicalName());
        Array arr = (Array) rs.getObject(1);
        Assert.assertEquals(((int[]) arr.getArray())[0], 42);
        Assert.assertEquals(((int[]) arr.getArray())[1], 23);

    }

    @Test(groups = "integration")
    public void testInsertQueryUUIDArray() throws SQLException {
        // issue #569
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS uuidArray");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS uuidArray"
          + "(id UInt8, arr Array(UUID)) "
          + "ENGINE = TinyLog");
        connection.createStatement().execute(
            "INSERT INTO uuidArray VALUES(1, ['5ff22319-793d-4e6c-bdc1-916095a5a496'])");
        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT * FROM uuidArray");
        rs.next();
        assertEquals(
            rs.getArray(2).getArray(),
            new UUID[] {UUID.fromString("5ff22319-793d-4e6c-bdc1-916095a5a496")});
    }

    @Test(groups = "integration")
    public void testMultiStatements() throws SQLException {
        try (Statement s = connection.createStatement()) {
            String sql = "select 1; select 2";
            try (ResultSet rs = s.executeQuery(sql)) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "2");
                assertFalse(rs.next());
            }

            assertTrue(s.execute(sql));
            try (ResultSet rs = s.getResultSet()) {
                assertNotNull(rs);
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "2");
                assertFalse(rs.next());
            }
            
            assertEquals(s.executeUpdate(sql), 1);
        }
    }

    @Test(groups = "integration")
    public void testBatchProcessing() throws SQLException {
        try (Statement s = connection.createStatement()) {
            int[] results = s.executeBatch();
            assertNotNull(results);
            assertEquals(results.length, 0);

            s.addBatch("select 1; select 2");
            s.addBatch("select 3");
            results = s.executeBatch();
            assertNotNull(results);
            assertEquals(results.length, 3);

            s.clearBatch();
            results = s.executeBatch();
            assertNotNull(results);
            assertEquals(results.length, 0);
        }
    }

    private static String readQueryId(ClickHouseStatementImpl stmt, long timeoutSecs) {
        long start = System.currentTimeMillis();
        String value;
        do {
            value = stmt.getQueryId();
        } while (value == null && TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) < timeoutSecs);

        return value;
    }

    private boolean checkQuery(String queryId, boolean isRunning, long timeoutSecs) throws Exception {
        long start = System.currentTimeMillis();

        do {
            ClickHouseStatement statement = null;
            try {
                statement = connection.createStatement();
                statement.execute(String.format("SELECT * FROM system.processes where query_id='%s'", queryId));
                ResultSet resultSet = statement.getResultSet();
                if (resultSet.next() == isRunning) {
                    return true;
                }
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }

        } while (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) < timeoutSecs);

        return false;
    }


}
