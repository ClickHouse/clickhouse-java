package com.clickhouse.client.api.transport;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ClientNodeSelectorTest {

    @Test
    public void testEndpointStateQuarantineAndExpiry() throws InterruptedException {
        Endpoint ep = new HttpEndpoint("localhost", 8123, false, "/");
        EndpointState state = new EndpointState(ep);

        Assert.assertTrue(state.isAlive(), "New endpoint state should be alive");
        Assert.assertSame(state.getEndpoint(), ep);

        state.markFailed(20);
        Assert.assertFalse(state.isAlive(), "Endpoint state should not be alive immediately after failure");

        Thread.sleep(40);
        Assert.assertTrue(state.isAlive(), "Endpoint state should be alive again after quarantine expires");
    }

    @Test
    public void testClientNodeSelectorAffinityAndQuarantine() {
        Endpoint epA = new HttpEndpoint("localhost", 8123, false, "/");
        Endpoint epB = new HttpEndpoint("localhost", 8124, false, "/");
        Endpoint epC = new HttpEndpoint("localhost", 8125, false, "/");

        ClientNodeSelector selector = new ClientNodeSelector(Arrays.asList(epA, epB, epC));

        Assert.assertEquals(selector.getEndpoint(), epA);

        Endpoint next = selector.getNextAliveNode(epA);
        Assert.assertEquals(next, epB);
        Assert.assertEquals(selector.getEndpoint(), epB);

        next = selector.getNextAliveNode(epB);
        Assert.assertEquals(next, epC);
        Assert.assertEquals(selector.getEndpoint(), epC);

        next = selector.getNextAliveNode(epC);
        Assert.assertEquals(next, epA);
        Assert.assertEquals(selector.getEndpoint(), epA);
    }

    @Test
    public void testClientNodeSelectorFallbackWhenAllDead() {
        Endpoint epA = new HttpEndpoint("localhost", 8123, false, "/");
        Endpoint epB = new HttpEndpoint("localhost", 8124, false, "/");

        ClientNodeSelector selector = new ClientNodeSelector(Arrays.asList(epA, epB));

        selector.getNextAliveNode(epA);
        selector.getNextAliveNode(epB);

        Assert.assertEquals(selector.getEndpoint(), epA);
    }

    @Test
    public void testClientNodeSelectorSequentialFailures() {
        Endpoint epA = new HttpEndpoint("localhost", 8123, false, "/");
        Endpoint epB = new HttpEndpoint("localhost", 8124, false, "/");
        Endpoint epC = new HttpEndpoint("localhost", 8125, false, "/");

        ClientNodeSelector selector = new ClientNodeSelector(Arrays.asList(epA, epB, epC));

        Assert.assertEquals(selector.getEndpoint(), epA);

        Assert.assertEquals(selector.getNextAliveNode(epA), epB);
        Assert.assertEquals(selector.getNextAliveNode(epB), epC);
        Assert.assertEquals(selector.getNextAliveNode(epC), epA);
    }

    @Test
    public void testClientNodeSelectorConcurrency() throws Exception {
        Endpoint epA = new HttpEndpoint("localhost", 8123, false, "/");
        Endpoint epB = new HttpEndpoint("localhost", 8124, false, "/");
        Endpoint epC = new HttpEndpoint("localhost", 8125, false, "/");

        ClientNodeSelector selector = new ClientNodeSelector(Arrays.asList(epA, epB, epC));

        int threadCount = 10;
        int loopCount = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for (int j = 0; j < loopCount; j++) {
                        Endpoint ep = selector.getEndpoint();
                        Assert.assertNotNull(ep);
                        if (j % 10 == 0) {
                            selector.getNextAliveNode(ep);
                        }
                    }
                    return null;
                }
            }));
        }

        executor.shutdown();
        Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        for (Future<Void> future : futures) {
            future.get(); 
        }
    }
}
