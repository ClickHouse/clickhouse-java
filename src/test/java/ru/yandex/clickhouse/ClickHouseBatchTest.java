package ru.yandex.clickhouse;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class ClickHouseBatchTest {
    private ClickHouseDataSource dataSource;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
    }

    @Test
    public void simpleBatchSelectTest() throws Exception {
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
        ClickHouseStatement statement = connection.createStatement();
        statement.addBatch("SELECT 1");
        statement.addBatch("SELECT 2");
        statement.addBatch("SELECT 3");
        statement.addBatch("SELECT 4");
        statement.addBatch("SELECT 5");
        statement.addBatch("SELECT 6");
        statement.addBatch("SELECT 7");
        statement.executeBatch();
    }
}
