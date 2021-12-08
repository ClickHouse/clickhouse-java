package com.clickhouse.jdbc;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;

import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.data.ClickHouseBitmap;
import com.clickhouse.client.data.ClickHouseExternalTable;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHousePreparedStatementTest extends JdbcIntegrationTest {
    @DataProvider(name = "typedParameters")
    private Object[][] getTypedParameters() {
        return new Object[][] {
                new Object[] { "Array(DateTime32)", new LocalDateTime[] { LocalDateTime.of(2021, 11, 1, 1, 2, 3),
                        LocalDateTime.of(2021, 11, 2, 2, 3, 4) } } };
    }

    @Test(groups = "integration")
    public void testBatchInsert() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn.prepareStatement("insert into test_batch_insert values(?,?)")) {
            conn.createStatement().execute("drop table if exists test_batch_insert;"
                    + "create table test_batch_insert(id Int32, name Nullable(String))engine=Memory");
            stmt.setInt(1, 1);
            stmt.setString(2, "a");
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.setString(2, "b");
            stmt.addBatch();
            stmt.setInt(1, 3);
            stmt.setString(2, null);
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 0, 0, 0 });
        }
    }

    @Test(groups = "integration")
    public void testBatchInput() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement(
                        "insert into test_batch_input select id, name, value from input('id Int32, name Nullable(String), desc Nullable(String), value AggregateFunction(groupBitmap, UInt32)')")) {
            s.execute("drop table if exists test_batch_input;"
                    + "create table test_batch_input(id Int32, name Nullable(String), value AggregateFunction(groupBitmap, UInt32))engine=Memory");
            Object[][] objs = new Object[][] {
                    new Object[] { 1, "a", "aaaaa", ClickHouseBitmap.wrap(1, 2, 3, 4, 5) },
                    new Object[] { 2, "b", null, ClickHouseBitmap.wrap(6, 7, 8, 9, 10) },
                    new Object[] { 3, null, "33333", ClickHouseBitmap.wrap(11, 12, 13) }
            };
            for (Object[] v : objs) {
                stmt.setInt(1, (int) v[0]);
                stmt.setString(2, (String) v[1]);
                stmt.setString(3, (String) v[2]);
                stmt.setObject(4, v[3]);
                stmt.addBatch();
            }
            stmt.executeBatch();

            try (ResultSet rs = s.executeQuery("select * from test_batch_input order by id")) {
                Object[][] values = new Object[objs.length][];
                int index = 0;
                while (rs.next()) {
                    values[index++] = new Object[] {
                            rs.getObject(1), rs.getObject(2), rs.getObject(3)
                    };
                }
                Assert.assertEquals(index, objs.length);
                for (int i = 0; i < objs.length; i++) {
                    Object[] actual = values[i];
                    Object[] expected = objs[i];
                    Assert.assertEquals(actual[0], expected[0]);
                    Assert.assertEquals(actual[1], expected[1]);
                    Assert.assertEquals(actual[2], expected[3]);
                }
            }
        }
    }

    @Test(groups = "integration")
    public void testBatchQuery() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn.prepareStatement("select * from numbers(100) where number < ?")) {
            Assert.assertThrows(SQLException.class, () -> stmt.setInt(0, 5));
            Assert.assertThrows(SQLException.class, () -> stmt.setInt(2, 5));
            Assert.assertThrows(SQLException.class, () -> stmt.addBatch());

            stmt.setInt(1, 3);
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 0, 0 });
        }
    }

    @Test(groups = "integration")
    public void testQueryWithExternalTable() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT bitmapContains(my_bitmap, toUInt32(1)) as v1, bitmapContains(my_bitmap, toUInt32(2)) as v2 from {tt 'ext_table'}")) {
            stmt.setObject(1, ClickHouseExternalTable.builder().name("ext_table")
                    .columns("my_bitmap AggregateFunction(groupBitmap,UInt32)").format(ClickHouseFormat.RowBinary)
                    .content(new ByteArrayInputStream(ClickHouseBitmap.wrap(1, 3, 5).toBytes()))
                    .asTempTable()
                    .build());
            ResultSet rs = stmt.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getInt(2), 0);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(dataProvider = "typedParameters", groups = "integration")
    public void testArrayParameter(String t, Object v) throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn.prepareStatement("select ?::?")) {
            stmt.setObject(1, v);
            // stmt.setString(2, t) or stmt.setObject(2, t) will result in quoted string
            stmt.setObject(2, new StringBuilder(t));
            ResultSet rs = stmt.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1), v);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testQueryWithNamedParameter() throws SQLException {
        Properties props = new Properties();
        props.setProperty(JdbcConfig.PROP_NAMED_PARAM, "true");
        LocalDateTime ts = LocalDateTime.ofEpochSecond(10000, 123456789, ZoneOffset.UTC);
        try (ClickHouseConnection conn = newConnection(props);
                PreparedStatement stmt = conn
                        .prepareStatement("select :ts1 ts1, :ts2(DateTime32) ts2")) {
            stmt.setObject(1, ts);
            stmt.setObject(2, ts);
            ResultSet rs = stmt.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "1970-01-01 02:46:40.123456789");
            Assert.assertEquals(rs.getString(2), "1970-01-01 02:46:40");
            Assert.assertFalse(rs.next());
        }
    }
}
