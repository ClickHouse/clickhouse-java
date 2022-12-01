package com.clickhouse.client.http;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.config.ClickHouseSocketOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class ApacheHttpConnectionImplTest extends BaseIntegrationTest {
    @Test(groups = { "integration" })
    public void testConnection() throws IOException {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseRequest<?> req = client.connect(server);
            req.option(ClickHouseHttpOption.CONNECTION_PROVIDER, HttpConnectionProvider.APACHE_HTTP_CLIENT);

            ClickHouseHttpConnection conn = ClickHouseHttpConnectionFactory.createConnection(server, req, null);
            Assert.assertNotNull(conn);
        }
    }
}
