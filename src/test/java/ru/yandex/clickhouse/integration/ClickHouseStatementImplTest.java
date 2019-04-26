package ru.yandex.clickhouse.integration;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.internal.util.reflection.Whitebox;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseExternalData;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

public class ClickHouseStatementImplTest {
    private ClickHouseDataSource dataSource;
    private ClickHouseConnection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
    }

    @AfterTest
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testUpdateCountForSelect() throws Exception {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT dummy FROM system.one");
        Assert.assertEquals(stmt.getUpdateCount(), -1);
        rs.next();
        Assert.assertEquals(stmt.getUpdateCount(), -1);
        rs.close();
        stmt.close();
    }

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
    public void testExternalData() throws SQLException, UnsupportedEncodingException {
        ClickHouseStatement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(
                "select UserName, GroupName " +
                        "from (select 'User' as UserName, 1 as GroupId) " +
                        "any left join groups using GroupId",
                null,
                Collections.singletonList(new ClickHouseExternalData(
                        "groups",
                        new ByteArrayInputStream("1\tGroup".getBytes())
                ).withStructure("GroupId UInt8, GroupName String"))
        );

        rs.next();

        String userName = rs.getString("UserName");
        String groupName = rs.getString("GroupName");

        Assert.assertEquals(userName, "User");
        Assert.assertEquals(groupName, "Group");
    }

    @Test
    public void testResultSetWithExtremes() throws SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setExtremes(true);
        ClickHouseDataSource dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
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

    @Test
    public void testSelectManyRows() throws SQLException {
        Statement stmt = connection.createStatement();
        int limit = 10000;
        ResultSet rs = stmt.executeQuery("select concat('test', toString(number)) as str from system.numbers limit " + limit);
        int i = 0;
        while (rs.next()) {
            String s = rs.getString("str");
            Assert.assertEquals(s, "test" + i);
            i++;
        }
        Assert.assertEquals(i, limit);
    }

    @Test
    public void testMoreResultsWithResultSet() throws SQLException {
        Statement stmt = connection.createStatement();

        Assert.assertTrue(stmt.execute("select 1"));
        Assert.assertNotNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), -1);

        Assert.assertFalse(stmt.getMoreResults());
        Assert.assertNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), -1);
    }

    @Test
    public void testMoreResultsWithUpdateCount() throws SQLException {
        Statement stmt = connection.createStatement();

        Assert.assertFalse(stmt.execute("create database if not exists default"));
        Assert.assertNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), 0);

        Assert.assertFalse(stmt.getMoreResults());
        Assert.assertNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), -1);
    }

    @Test
    public void testSelectQueryStartingWithWith() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("WITH 2 AS two SELECT two * two;");

        Assert.assertNotNull(rs);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt(1), 4);

        rs.close();
        stmt.close();
    }

    @Test
    public void cancelTest_queryId_is_not_set() throws Exception {
        final ClickHouseStatement firstStatement = dataSource.getConnection().createStatement();

        final AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<Exception>();
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    Map<ClickHouseQueryParam, String> params = new EnumMap<ClickHouseQueryParam, String>(ClickHouseQueryParam.class);
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
        String queryId = (String) readField(firstStatement, "queryId", timeout);
        assertNotNull(String.format("it's actually very strange. It seems the query hasn't been executed in %s seconds", timeout), queryId);
        assertNull("An exception happened while the query was being executed", exceptionAtomicReference.get());


        assertTrue("The query isn't being executed. It seems very strange", checkQuery(queryId, true,10));
        firstStatement.cancel();
        assertTrue("The query is still being executed", checkQuery(queryId, false, 10));

        firstStatement.close();
        thread.interrupt();
    }


    @Test
    public void cancelTest_queryId_is_set() throws Exception {
        final String queryId = UUID.randomUUID().toString();
        final ClickHouseStatement firstStatement = dataSource.getConnection().createStatement();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<Exception>();
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    Map<ClickHouseQueryParam, String> params = new EnumMap<ClickHouseQueryParam, String>(ClickHouseQueryParam.class);
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
        assertTrue(String.format("it's actually very strange. It seems the query hasn't been executed in %s seconds", timeout), countDownLatch.await(timeout, TimeUnit.SECONDS));
        assertNull("An exception happened while the query was being executed", exceptionAtomicReference.get());

        assertTrue("The query isn't being executed. It seems very strange", checkQuery(queryId, true,10));
        firstStatement.cancel();
        assertTrue("The query is still being executed", checkQuery(queryId, false, 10));

        firstStatement.close();
        thread.interrupt();
    }

    @Test
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

    private static Object readField(Object object, String fieldName, long timeoutSecs) {
        long start = System.currentTimeMillis();
        Object value;
        do {
            value = Whitebox.getInternalState(object, fieldName);
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
