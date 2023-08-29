package com.clickhouse.client.http;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DefaultHttpConnectionTest extends BaseIntegrationTest {
    @Test(groups = { "integration" })
    public void testConnectionReuse() throws IOException {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseRequest<?> req = client.read(server);

            ClickHouseHttpConnection conn = ClickHouseHttpConnectionFactory.createConnection(server, req, null);
            Assert.assertNotNull(conn);
        }
    }
}
