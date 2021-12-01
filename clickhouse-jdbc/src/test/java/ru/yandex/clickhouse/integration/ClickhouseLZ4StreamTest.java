package ru.yandex.clickhouse.integration;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.JdbcIntegrationTest;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class ClickhouseLZ4StreamTest extends JdbcIntegrationTest {
    private ClickHouseConnection connection;

    @BeforeClass(groups = "integration")
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setDecompress(true);
        
        connection = newConnection(properties);
    }

    @AfterClass(groups = "integration")
    public void tearDown() throws Exception {
        closeConnection(connection);
    }

    @Test(groups = "integration")
    public void testBigBatchCompressedInsert() throws SQLException {
        if ("21.3.3.14".equals(connection.getServerVersion())) {
            return;
        }

        connection.createStatement().execute("DROP TABLE IF EXISTS big_batch_insert");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS big_batch_insert (i Int32, s String) ENGINE = TinyLog"
        );

        PreparedStatement statement = connection.prepareStatement("INSERT INTO big_batch_insert (s, i) VALUES (?, ?)");

        int cnt = 1000000;
        for (int i = 0; i < cnt; i++) {
            statement.setString(1, "string" + i);
            statement.setInt(2, i);
            statement.addBatch();
        }

        statement.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() as cnt from big_batch_insert");
        rs.next();
        Assert.assertEquals(rs.getInt("cnt"), cnt);
        Assert.assertFalse(rs.next());
    }
}
