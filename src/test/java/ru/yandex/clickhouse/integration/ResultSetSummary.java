package ru.yandex.clickhouse.integration;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class ResultSetSummary {
    private ClickHouseConnection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
        connection.createStatement().execute("DROP TABLE IF EXISTS test.insert_test");
        connection.createStatement().execute("CREATE TABLE IF NOT EXISTS test.insert_test (value UInt32) ENGINE = TinyLog");
    }

    @AfterTest
    public void tearDown() throws Exception {
        connection.createStatement().execute("DROP DATABASE test");
    }

    @Test
    public void select() throws Exception {
        ClickHouseStatement st = connection.createStatement();
        st.executeQuery("SELECT * FROM numbers(10)", Collections.singletonMap(ClickHouseQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertEquals(st.getResponseSummary().getReadRows(), 10);
        assertTrue(st.getResponseSummary().getReadBytes() > 0);
    }

    @Test
    public void selectWithoutParam() throws Exception {
        ClickHouseStatement st = connection.createStatement();
        st.executeQuery("SELECT * FROM numbers(10)", Collections.singletonMap(ClickHouseQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertEquals(st.getResponseSummary().getReadRows(), 10);
        assertTrue(st.getResponseSummary().getReadBytes() > 0);
    }

    @Test
    public void insertSingle() throws Exception {
        ClickHousePreparedStatement ps = (ClickHousePreparedStatement) connection.prepareStatement("INSERT INTO test.insert_test VALUES(?)");
        ps.setLong(1, 1);
        ps.executeQuery(Collections.singletonMap(ClickHouseQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertEquals(ps.getResponseSummary().getWrittenRows(), 1);
        assertTrue(ps.getResponseSummary().getWrittenBytes() > 0);
    }

    @Test
    public void insertBatch() throws Exception {
        ClickHousePreparedStatement ps = (ClickHousePreparedStatement) connection.prepareStatement("INSERT INTO test.insert_test VALUES(?)");
        for (long i = 0; i < 10; i++) {
            ps.setLong(1, i);
            ps.addBatch();
        }
        ps.executeBatch(Collections.singletonMap(ClickHouseQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertEquals(ps.getResponseSummary().getWrittenRows(), 10);
        assertTrue(ps.getResponseSummary().getWrittenBytes() > 0);
    }

    @Test
    public void insertSelect() throws Exception {
        ClickHousePreparedStatement ps = (ClickHousePreparedStatement) connection.prepareStatement("INSERT INTO test.insert_test SELECT number FROM numbers(10)");
        ps.executeQuery(Collections.singletonMap(ClickHouseQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertEquals(ps.getResponseSummary().getWrittenRows(), 10);
        assertTrue(ps.getResponseSummary().getWrittenBytes() > 0);
    }
}
