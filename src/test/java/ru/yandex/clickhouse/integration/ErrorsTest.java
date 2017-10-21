package ru.yandex.clickhouse.integration;

import com.google.common.base.Throwables;
import org.testng.Assert;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class ErrorsTest {

    @Test
    public void testWrongUser() throws SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser("not_existing");
        DataSource dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        try {
            Connection connection = dataSource.getConnection();
        } catch (Exception e) {
            List<Throwable> causalChain = Throwables.getCausalChain(e);
            for (Throwable throwable : causalChain) {
                if (throwable instanceof ClickHouseException) {
                    Assert.assertEquals(((ClickHouseException) throwable).getErrorCode(), 192);
                    return;
                }
            }
        }
        Assert.assertTrue(false, "didn' find correct error");
    }

    @Test(expectedExceptions = ClickHouseException.class)
    public void testTableNotExists() throws SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        DataSource dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("select * from table_not_exists");
    }
}
