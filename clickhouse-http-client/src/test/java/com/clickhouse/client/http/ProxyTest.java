package com.clickhouse.client.http;

import com.clickhouse.client.*;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProxyTest extends BaseIntegrationTest {

    private static  ToxiproxyContainer toxiproxy = null;

    @BeforeSuite(groups = { "integration" })
    public static void beforeSuite() throws IOException {
        Network network = ClickHouseServerForTest.getNetwork();
        toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
                .withNetwork(network);
        toxiproxy.start();
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        toxiproxyClient.createProxy("clickhouse", "0.0.0.0:8666", String.format("clickhouse:%d", ClickHouseProtocol.HTTP.getDefaultPort()));

    }

    @Test(groups = { "integration" })
    public void testProxyConnection() throws IOException {


        String proxyHost = toxiproxy.getHost();
        int proxyPort = toxiproxy.getMappedPort(8666);        Map<String, String> options = new HashMap<>();
        options.put("proxy_type", "HTTP");
        options.put("proxy_hostname", proxyHost);
        options.put("proxy_port", Integer.toString(proxyPort));

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP, options);
        ClickHouseClient clientPing = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
        boolean b = clientPing.ping(server, 30000);
        Assert.assertEquals(b , true, "Can not ping via proxy");

    }

    @AfterSuite(groups = { "integration" })
    public static void afterSuite() {
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
    }

}
