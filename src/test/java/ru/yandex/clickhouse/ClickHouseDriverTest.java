package ru.yandex.clickhouse;

import java.sql.SQLException;
import java.util.Properties;

import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;

public class ClickHouseDriverTest {
    @Test
    public void connectShouldReturnNullForUnsupportedUrl() throws SQLException {
        assertNull(new ClickHouseDriver().connect("jdbc:mysql:localhost", new Properties()));
    }
}