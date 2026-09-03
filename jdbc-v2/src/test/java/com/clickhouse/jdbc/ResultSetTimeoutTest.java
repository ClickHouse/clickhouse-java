package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.hc.core5.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Properties;

public class ResultSetTimeoutTest {

    @Test
    public void shouldMapServerTimeoutWhileReadingResultSet() throws Exception {
        String exceptionTag = "0123456789abcdef";
        String queryId = "result-set-timeout";
        String errorMessage = "Code: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)";
        String exceptionFrame = "\r\n__exception__\r\n" + exceptionTag + "\r\n" + errorMessage
                + "\r\n" + errorMessage.getBytes(StandardCharsets.UTF_8).length + " " + exceptionTag
                + "\r\n__exception__\r\n";
        // RowBinaryWithNamesAndTypes schema followed by two rows; next() prefetches one row ahead.
        byte[] rowBinaryResultPrefix = {
                0x01, 0x01, 0x31, 0x05, 0x55, 0x49, 0x6e, 0x74, 0x38, 0x01, 0x02
        };
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(rowBinaryResultPrefix);
        body.write(exceptionFrame.getBytes(StandardCharsets.UTF_8));

        WireMockServer mockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        mockServer.start();

        try {
            mockServer.stubFor(WireMock.post(WireMock.anyUrl())
                    .willReturn(WireMock.aResponse()
                            .withStatus(HttpStatus.SC_OK)
                            .withHeader(ClickHouseHttpProto.HEADER_EXCEPTION_TAG, exceptionTag)
                            .withHeader(ClickHouseHttpProto.HEADER_QUERY_ID, queryId)
                            .withBody(body.toByteArray())));

            Properties properties = new Properties();
            properties.setProperty(ClientConfigProperties.SERVER_TIMEZONE.getKey(), "UTC");
            properties.setProperty(ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getKey(), "false");
            properties.setProperty(ClientConfigProperties.USE_HTTP_COMPRESSION.getKey(), "false");

            String jdbcUrl = "jdbc:clickhouse://localhost:" + mockServer.port() + "/default";
            try (Connection connection = new ConnectionImpl(jdbcUrl, properties);
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT 1")) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(resultSet.getInt(1), 1);

                SQLTimeoutException exception = Assert.expectThrows(SQLTimeoutException.class, resultSet::next);
                Assert.assertEquals(exception.getErrorCode(), 159);
                Assert.assertEquals(exception.getSQLState(), ExceptionUtils.SQL_STATE_TIMEOUT);
                Assert.assertTrue(ExceptionUtils.getRootCause(exception) instanceof ServerException);
                Assert.assertEquals(((ServerException) ExceptionUtils.getRootCause(exception)).getCode(), 159);
                Assert.assertTrue(exception.getMessage().startsWith(errorMessage), exception.getMessage());
            }
        } finally {
            mockServer.stop();
        }
    }
}
