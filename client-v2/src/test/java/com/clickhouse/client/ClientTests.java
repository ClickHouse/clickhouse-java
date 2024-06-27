package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.util.Optional;

public class ClientTests extends BaseIntegrationTest {


    @Test(enabled = false)
    public void testWithSSL() {

        ClickHouseNode secureNode = getSecureServer(ClickHouseProtocol.HTTP);
        Client client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, "localhost", secureNode.getPort(), true)
                .setUsername("default")
                .setPassword("")
                .build();


        Optional<GenericRecord> genericRecord = client
                .queryAll("SELECT hostname()").stream().findFirst();
        Assert.assertTrue(genericRecord.isPresent());

        System.out.println(genericRecord.get().getString(1));
    }
}
