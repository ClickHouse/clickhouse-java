package com.clickhouse.client.http;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.config.ClickHouseOption;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ApacheHttpConnectionImplTest extends ClickHouseHttpClientTest {
    @Override
    protected Map<ClickHouseOption, Serializable> getClientOptions() {
        return Collections.singletonMap(ClickHouseHttpOption.CONNECTION_PROVIDER,
                HttpConnectionProvider.APACHE_HTTP_CLIENT);
    }

    @Test(groups = { "integration" })
    public void testConnection() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (ClickHouseClient client = ClickHouseClient.newInstance()) {

            ClickHouseRequest<?> req1 = newRequest(client, server);
            try (ClickHouseResponse resp = req1.query("select 1").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asString(), "1");
            }

            // req2 will use same connection with req1
            ClickHouseRequest<?> req2 = newRequest(client, server);
            try (ClickHouseResponse resp = req2.query("select 1").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asString(), "1");
            }
        }
    }
}
