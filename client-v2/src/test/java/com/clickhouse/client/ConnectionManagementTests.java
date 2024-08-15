package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.query.GenericRecord;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.trafficlistener.WiremockNetworkTrafficListener;
import org.apache.hc.core5.net.URIBuilder;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionManagementTests extends BaseIntegrationTest{


    @Test(groups = {"integration"},dataProvider = "testConnectionTTLProvider")
    @SuppressWarnings("java:S2925")
    public void testConnectionTTL(Long connectionTtl, Long keepAlive, int openSockets) throws Exception {
        if (isCloud()) {
            return; // skip cloud tests because of wiremock proxy. TODO: fix it
        }
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        int proxyPort = new Random().nextInt(1000) + 10000;
        System.out.println("proxyPort: " + proxyPort);
        ConnectionCounterListener connectionCounter = new ConnectionCounterListener();
        WireMockServer proxy = new WireMockServer(WireMockConfiguration
                .options().port(proxyPort)
                .networkTrafficListener(connectionCounter)
                .notifier(new Slf4jNotifier(true)));
        proxy.start();
        URIBuilder targetURI = new URIBuilder(server.getBaseUri())
                .setPath("");
        proxy.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse().proxiedFrom(targetURI.build().toString())).build());

        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useNewImplementation(true)
                .addProxy(ProxyType.HTTP, "localhost", proxyPort);
        if (connectionTtl != null) {
            clientBuilder.setConnectionTTL(connectionTtl, ChronoUnit.MILLIS);
        }
        if (keepAlive != null) {
            clientBuilder.setKeepAliveTimeout(keepAlive, ChronoUnit.MILLIS);
        }

        try (Client client = clientBuilder.build()) {
            List<GenericRecord> resp = client.queryAll("select 1");
            Assert.assertEquals(resp.stream().findFirst().get().getString(1), "1");

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                Assert.fail("Unexpected exception", e);
            }

            resp = client.queryAll("select 1");
            Assert.assertEquals(resp.stream().findFirst().get().getString(1), "1");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception", e);
        } finally {
            Assert.assertEquals(connectionCounter.opened.get(), openSockets);
            proxy.stop();
        }
    }

    @DataProvider(name = "testConnectionTTLProvider")
    public static Object[][]  testConnectionTTLProvider() {
        return new Object[][] {
                { 1000L, null, 2 },
                { 2000L, null, 1 },
                { null, 2000L, 1 },
                { null, 500L, 2 },
                { 1000L, 0L, 2 },
                { 1000L, 3000L, 2}
        };
    }

    private static class ConnectionCounterListener implements WiremockNetworkTrafficListener {

        private AtomicInteger opened = new AtomicInteger(0);
        private AtomicInteger closed = new AtomicInteger(0);

        @Override
        public void opened(Socket socket) {
            opened.incrementAndGet();
        }

        @Override
        public void incoming(Socket socket, ByteBuffer bytes) {
            // ignore
        }

        @Override
        public void outgoing(Socket socket, ByteBuffer bytes) {
            // ignore
        }

        @Override
        public void closed(Socket socket) {
            closed.incrementAndGet();
        }
    }

}
