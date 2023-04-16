package com.clickhouse.r2dbc.connection;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.clickhouse.r2dbc.BaseR2dbcTest;

import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

public class ClickHouseConnectionTest extends BaseR2dbcTest {
    protected void executeQuery(String sql, String expectedStringResults, String customConnStr) {
        ConnectionFactory connectionFactory = getConnectionFactory(DEFAULT_PROTOCOL, EXTRA_PARAM, customConnStr);

        AtomicReference<Throwable> error = new AtomicReference<>();
        StringBuilder builder = new StringBuilder(64);
        Mono.from(connectionFactory.create())
                .flatMapMany(connection -> connection.createStatement(sql).execute())
                .flatMap(result -> result.map((row, rowMetadata) -> row.get(0)))
                .doOnNext(result -> builder.append(result))
                .doOnError(error::set)
                .then()
                .block();

        Assert.assertNull("Should not run into error", error.get());
        Assert.assertEquals(expectedStringResults, builder.toString());
    }

    // @Test
    public void testQuery() throws Exception {
        String sql = "SELECT * FROM numbers(10)";
        String expected = "0123456789";
        executeQuery(sql, expected, null);
        executeQuery(sql, expected, "format=CSV");
        executeQuery(sql, expected, "format=CSV&max_result_rows=11");
    }
}