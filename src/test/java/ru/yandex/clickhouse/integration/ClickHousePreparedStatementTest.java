package ru.yandex.clickhouse.integration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ClickHousePreparedStatementTest {
    private ClickHouseDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @Test(enabled = false)
    public void testArrayTest() throws Exception {

        connection.createStatement().execute("DROP TABLE IF EXISTS test.array_test");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.array_test (i Int32, a Array(Int32)) ENGINE = TinyLog"
        );

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.array_test (i, a) VALUES (?, ?)");

        statement.setInt(1, 1);
        statement.setArray(2, new ClickHouseArray(new int[]{1, 2, 3}));
        statement.addBatch();

        statement.setInt(1, 2);
        statement.setArray(2, new ClickHouseArray(new int[]{2, 3, 4, 5}));
        statement.addBatch();
        statement.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() as cnt from test.array_test");
        rs.next();

        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertFalse(rs.next());

    }

    @Test(enabled = false)
    public void testSingleColumnResultSet() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("select c from (\n" +
                "    select 'a' as c, 1 as rn\n" +
                "    UNION ALL select 'b' as c, 2 as rn\n" +
                "    UNION ALL select '' as c, 3 as rn\n" +
                "    UNION ALL select 'd' as c, 4 as rn\n" +
                " ) order by rn");
        StringBuffer sb = new StringBuffer();
        while(rs.next()){
            sb.append(rs.getString("c")).append("\n");
        }
        Assert.assertEquals(sb.toString(), "a\nb\n\nd\n");
    }
}
