package com.clickhouse.client.http;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ApacheHttpConnectionImplTest extends BaseIntegrationTest {
    @Test(groups = { "integration" })
    public void testConnection() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseRequest<?> req = client.connect(server);

            ClickHouseHttpConnection conn = ClickHouseHttpConnectionFactory.createConnection(server, req, null);
            Assert.assertNotNull(conn);
            conn.closeQuietly();

            ClickHouseRequest<?> req1 = client.connect(server);
            try (ClickHouseResponse resp = req1
                    .option(ClickHouseHttpOption.CONNECTION_PROVIDER, HttpConnectionProvider.APACHE_HTTP_CLIENT)
                    .query("select 1").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asString(), "1");
            }

            // req2 will use same connection with req1
            ClickHouseRequest<?> req2 = client.connect(server);
            try (ClickHouseResponse resp = req2
                    .option(ClickHouseHttpOption.CONNECTION_PROVIDER, HttpConnectionProvider.APACHE_HTTP_CLIENT)
                    .query("select 1").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asString(), "1");
            }

        }
    }
}
