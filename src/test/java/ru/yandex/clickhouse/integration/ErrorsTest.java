package ru.yandex.clickhouse.integration;

import com.google.common.base.Throwables;
import org.testng.Assert;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class ErrorsTest {

    @Test
    public void testWrongUser() {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser("not_existing");
        DataSource dataSource = ClickHouseContainerForTest.newDataSource(properties);
        try {
            Connection connection = dataSource.getConnection();
        } catch (Exception e) {
            Assert.assertEquals((getClickhouseException(e)).getErrorCode(), 516);
            return;
        }
        Assert.assertTrue(false, "didn' find correct error");
    }

    @Test(expectedExceptions = ClickHouseException.class)
    public void testTableNotExists() throws SQLException {
        DataSource dataSource = ClickHouseContainerForTest.newDataSource();
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("select * from table_not_exists");
    }

    @Test
    public void testErrorDecompression() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setCompress(true);
        String[] address = ClickHouseContainerForTest.getClickHouseHttpAddress().split(":");
        DataSource dataSource = ClickHouseContainerForTest.newDataSource(properties);
        Connection connection = dataSource.getConnection();

        connection.createStatement().execute("DROP TABLE IF EXISTS test.table_not_exists");

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.table_not_exists (d, s) VALUES (?, ?)");

        statement.setDate(1, new Date(System.currentTimeMillis()));
        statement.setInt(2, 1);
        try {
            statement.executeBatch();
        } catch (Exception e) {
            Assert.assertTrue(getClickhouseException(e).getMessage().startsWith("ClickHouse exception, code: 60, host: " + address[0] +", port: " + address[1] +"; Code: 60, e.displayText() = DB::Exception: Table test.table_not_exists doesn't exist."));
            return;
        }
        Assert.assertTrue(false, "didn' find correct error");
    }

    private static ClickHouseException getClickhouseException(Exception e) {
        List<Throwable> causalChain = Throwables.getCausalChain(e);
        for (Throwable throwable : causalChain) {
            if (throwable instanceof ClickHouseException) {
                return (ClickHouseException) throwable;
            }
        }
        throw new IllegalArgumentException("no ClickHouseException found");
    }
}
