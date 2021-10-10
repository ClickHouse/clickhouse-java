package ru.yandex.clickhouse.integration;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.JdbcIntegrationTest;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.ClickHouseVersion;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ErrorsTest extends JdbcIntegrationTest {
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
    public void testWrongUser() {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser("not_existing");
        try (Connection connection = newConnection(properties)) {
        } catch (Exception e) {
            String version = ClickHouseServerForTest.getClickHouseVersion();
            if (!version.isEmpty() && ClickHouseVersion.check(version, "(,19]")) {
                Assert.assertEquals((getClickhouseException(e)).getErrorCode(), 192);
            } else {
                Assert.assertEquals((getClickhouseException(e)).getErrorCode(), 516);
            }
            return;
        }
        Assert.assertTrue(false, "didn' find correct error");
    }

    @Test(groups = "integration", expectedExceptions = ClickHouseException.class)
    public void testTableNotExists() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("select * from table_not_exists");
        }
    }

    @Test(groups = "integration")
    public void testErrorDecompression() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setCompress(true);
        String[] address = getClickHouseHttpAddress().split(":");

        try (Connection connection = newConnection(properties)) {
            connection.createStatement().execute("DROP TABLE IF EXISTS table_not_exists");

            PreparedStatement statement = connection
                    .prepareStatement("INSERT INTO table_not_exists (d, s) VALUES (?, ?)");

            statement.setDate(1, new Date(System.currentTimeMillis()));
            statement.setInt(2, 1);
            try {
                statement.executeBatch();
            } catch (Exception e) {
                String exceptionMsg = getClickhouseException(e).getMessage();
                Assert.assertTrue(
                        exceptionMsg
                                .startsWith("ClickHouse exception, code: 60, host: " + address[0] + ", port: "
                                        + address[1] + "; Code: 60")
                                && exceptionMsg.contains(
                                        " DB::Exception: Table " + dbName + ".table_not_exists doesn't exist"),
                        exceptionMsg);
                return;
            }
            Assert.assertTrue(false, "didn' find correct error");
        }
    }

    private static ClickHouseException getClickhouseException(Throwable t) {
        List<Throwable> causes = new ArrayList<>(4);
        causes.add(t);

        Throwable slowPointer = t;
        boolean advanceSlowPointer = false;

        Throwable cause;
        while ((cause = t.getCause()) != null) {
            t = cause;
            causes.add(t);

            if (t == slowPointer) {
                throw new IllegalArgumentException("Loop in causal chain detected.", t);
            }
            if (advanceSlowPointer) {
                slowPointer = slowPointer.getCause();
            }
            advanceSlowPointer = !advanceSlowPointer; // only advance every other iteration
        }

        for (Throwable throwable : causes) {
            if (throwable instanceof ClickHouseException) {
                return (ClickHouseException) throwable;
            }
        }

        throw new IllegalArgumentException("no ClickHouseException found");
    }
}
