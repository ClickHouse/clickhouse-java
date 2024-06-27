package com.clickhouse.client.api.internal;

import com.clickhouse.client.ClickHouseNode;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class HttpAPIClientHelperTest {


    @Test
    public void testHostUnavailable() throws Exception {

        HttpAPIClientHelper clientHelper = new HttpAPIClientHelper(new HashMap<>());

        ClickHouseNode server1 = ClickHouseNode.of("http://lo1calhost:8000");
        ClickHouseNode server2 = ClickHouseNode.of("http://localhost:8000");
        ClickHouseNode server3 = ClickHouseNode.of("http://localhost:8123");

        CompletableFuture<ClassicHttpResponse> future = clientHelper.executeRequest(
                Arrays.asList(server1, server2, server3), "SELECT 1", null);

        ClassicHttpResponse httpResponse = future.get(10, TimeUnit.SECONDS);
        System.out.println(httpResponse);
    }
}