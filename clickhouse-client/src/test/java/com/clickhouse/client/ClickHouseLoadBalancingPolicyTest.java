package com.clickhouse.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.clickhouse.client.ClickHouseNode.Status;
import com.clickhouse.client.config.ClickHouseClientOption;

public class ClickHouseLoadBalancingPolicyTest {
    static class CustomPolicy extends ClickHouseLoadBalancingPolicy {
    }

    static class PrivatePolicy extends ClickHouseLoadBalancingPolicy {
        private PrivatePolicy() {
        }
    }

    static final int MAX_WAIT_SECONDS = 15;

    private ClickHouseNodes createNodes(int size, String policy) {
        ClickHouseNode template = ClickHouseNode.builder().host("test.host")
                .addOption(ClickHouseClientOption.LOAD_BALANCING_POLICY.getKey(), policy).build();

        ClickHouseNode[] nodes = new ClickHouseNode[size];
        for (int i = 0; i < size; i++) {
            nodes[i] = ClickHouseNode.builder(template).port(ClickHouseProtocol.HTTP, i + 1)
                    .tags(String.valueOf(i % size)).build();
        }

        return new ClickHouseNodes(Arrays.asList(nodes), template);
    }

    @DataProvider(name = "nodeSelectorProvider")
    private Object[][] getNodeSelectors() {
        return new Object[][] { { null }, { ClickHouseNodeSelector.EMPTY },
                { ClickHouseNodeSelector.of(Collections.emptyList(), Collections.singleton("3")) } };
    }

    @Test(groups = { "unit" })
    public void testGetOrCreate() {
        Assert.assertEquals(ClickHouseLoadBalancingPolicy.of(null),
                ClickHouseLoadBalancingPolicy.of(""));
        Assert.assertEquals(ClickHouseLoadBalancingPolicy.of(""),
                ClickHouseLoadBalancingPolicy.of(null));
        Assert.assertEquals(ClickHouseLoadBalancingPolicy.of("firstalive"),
                ClickHouseLoadBalancingPolicy.of(ClickHouseLoadBalancingPolicy.FIRST_ALIVE));
        Assert.assertEquals(ClickHouseLoadBalancingPolicy.of("firstAlive"),
                ClickHouseLoadBalancingPolicy.of(ClickHouseLoadBalancingPolicy.FIRST_ALIVE));
        Assert.assertEquals(ClickHouseLoadBalancingPolicy.of("RANDOM"),
                ClickHouseLoadBalancingPolicy.of(ClickHouseLoadBalancingPolicy.RANDOM));
        Assert.assertEquals(ClickHouseLoadBalancingPolicy.of("RoundRobin"),
                ClickHouseLoadBalancingPolicy.of(ClickHouseLoadBalancingPolicy.ROUND_ROBIN));
        // custom policy
        Assert.assertEquals(ClickHouseLoadBalancingPolicy.of(CustomPolicy.class.getName()),
                ClickHouseLoadBalancingPolicy.of(CustomPolicy.class.getName()));
        // invalid
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseLoadBalancingPolicy.of(getClass().getName()));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseLoadBalancingPolicy.of(PrivatePolicy.class.getName()));
    }

    @Test(dataProvider = "nodeSelectorProvider", groups = { "unit" })
    public void testDummy(ClickHouseNodeSelector nodeSelector) throws InterruptedException {
        int size = 5;
        int requests = 500;
        int len = size * requests;
        int tag = nodeSelector != null && nodeSelector.getPreferredTags().size() > 0
                ? Integer.parseInt(nodeSelector.getPreferredTags().iterator().next())
                : -1;

        ClickHouseNodes nodes = createNodes(size, "");

        // single thread
        int[] counters = new int[size];
        for (int i = 0; i < len; i++) {
            ClickHouseNode node = nodes.apply(nodeSelector);
            counters[node.getPort() - 1] += 1;
        }

        for (int i = 0; i < size; i++) {
            if (tag == -1) {
                Assert.assertEquals(counters[i], i == 0 ? len : 0);
            } else if (i == tag) {
                Assert.assertEquals(counters[i], len);
            } else {
                Assert.assertEquals(counters[i], 0);
            }
        }

        // multi-thread
        CountDownLatch latch = new CountDownLatch(len);
        List<ClickHouseNode> results = Collections.synchronizedList(new ArrayList<>(len));
        ExecutorService executor = Executors.newFixedThreadPool(3);
        for (int i = 0; i < len; i++) {
            executor.execute(() -> {
                ClickHouseNode node = nodes.apply(nodeSelector);
                results.add(node);
                latch.countDown();
            });
        }

        if (!latch.await(MAX_WAIT_SECONDS, TimeUnit.SECONDS)) {
            Assert.fail("Failed to complete operation within " + MAX_WAIT_SECONDS
                    + " seconds - check exception log to see what's going on");
        }
        counters = new int[size];
        for (int i = 0; i < len; i++) {
            counters[results.get(i).getPort() - 1] += 1;
        }
        for (int i = 0; i < size; i++) {
            if (tag == -1) {
                Assert.assertEquals(counters[i], i == 0 ? len : 0);
            } else if (i == tag) {
                Assert.assertEquals(counters[i], len);
            } else {
                Assert.assertEquals(counters[i], 0);
            }
        }
    }

    @Test(dataProvider = "nodeSelectorProvider", groups = { "unit" })
    public void testFirstAlive(ClickHouseNodeSelector nodeSelector) throws InterruptedException {
        int size = 5;
        int requests = 500;
        int len = size * requests;
        int tag = nodeSelector != null && nodeSelector.getPreferredTags().size() > 0
                ? Integer.parseInt(nodeSelector.getPreferredTags().iterator().next())
                : -1;

        ClickHouseNodes nodes = createNodes(size, ClickHouseLoadBalancingPolicy.FIRST_ALIVE);

        // single thread
        int[] counters = new int[size];
        for (int i = 0; i < len; i++) {
            ClickHouseNode node = nodes.apply(nodeSelector);
            counters[node.getPort() - 1] += 1;
        }

        for (int i = 0; i < size; i++) {
            if (tag == -1) {
                Assert.assertEquals(counters[i], i == 0 ? len : 0);
            } else if (i == tag) {
                Assert.assertEquals(counters[i], len);
            } else {
                Assert.assertEquals(counters[i], 0);
            }
        }

        // multi-thread
        CountDownLatch latch = new CountDownLatch(len);
        List<ClickHouseNode> results = Collections.synchronizedList(new ArrayList<>(len));
        ExecutorService executor = Executors.newFixedThreadPool(3);
        for (int i = 0; i < len; i++) {
            executor.execute(() -> {
                ClickHouseNode node = nodes.apply(nodeSelector);
                results.add(node);
                latch.countDown();
            });
        }

        if (!latch.await(MAX_WAIT_SECONDS, TimeUnit.SECONDS)) {
            Assert.fail("Failed to complete operation within " + MAX_WAIT_SECONDS
                    + " seconds - check exception log to see what's going on");
        }
        counters = new int[size];
        for (int i = 0; i < len; i++) {
            counters[results.get(i).getPort() - 1] += 1;
        }
        for (int i = 0; i < size; i++) {
            if (tag == -1) {
                Assert.assertEquals(counters[i], i == 0 ? len : 0);
            } else if (i == tag) {
                Assert.assertEquals(counters[i], len);
            } else {
                Assert.assertEquals(counters[i], 0);
            }
        }
    }

    @Test(dataProvider = "nodeSelectorProvider", groups = { "unit" })
    public void testFirstAliveWithFailures(ClickHouseNodeSelector nodeSelector) throws InterruptedException {
        int size = 5;
        int requests = 500;
        int len = size * requests;
        int tag = nodeSelector != null && nodeSelector.getPreferredTags().size() > 0
                ? Integer.parseInt(nodeSelector.getPreferredTags().iterator().next())
                : -1;

        ClickHouseNodes nodes = createNodes(size, ClickHouseLoadBalancingPolicy.FIRST_ALIVE);

        CountDownLatch latch = new CountDownLatch(len);
        List<ClickHouseNode> results = Collections.synchronizedList(new ArrayList<>(len));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        for (int i = 0; i < len; i++) {
            executor.execute(() -> {
                ClickHouseNode node = nodes.apply(nodeSelector);
                results.add(node);
                latch.countDown();
            });
        }

        // change then first node to faulty
        nodes.nodes.get(0).update(Status.FAULTY);
        // remove the second
        nodes.nodes.get(1).update(Status.STANDALONE);
        // change the first back to healthy
        nodes.nodes.get(0).update(Status.HEALTHY);

        if (!latch.await(MAX_WAIT_SECONDS, TimeUnit.SECONDS)) {
            Assert.fail("Failed to complete operation within " + MAX_WAIT_SECONDS
                    + " seconds - check exception log to see what's going on");
        }
        int[] counters = new int[size];
        for (int i = 0; i < len; i++) {
            counters[results.get(i).getPort() - 1] += 1;
        }
        for (int i = 0; i < size; i++) {
            if (tag == -1) {
                Assert.assertTrue(i < 3 ? counters[i] >= 0 : counters[i] == 0,
                        "Only the first 3 nodes can be selected");
            } else if (i == tag) {
                Assert.assertEquals(counters[i], len);
            } else {
                Assert.assertEquals(counters[i], 0);
            }
        }
    }

    @Test(dataProvider = "nodeSelectorProvider", groups = { "unit" })
    public void testRoundRobin(ClickHouseNodeSelector nodeSelector) throws InterruptedException {
        int size = 5;
        int requests = 500;
        int len = size * requests;
        int tag = nodeSelector != null && nodeSelector.getPreferredTags().size() > 0
                ? Integer.parseInt(nodeSelector.getPreferredTags().iterator().next())
                : -1;

        ClickHouseNodes nodes = createNodes(size, ClickHouseLoadBalancingPolicy.ROUND_ROBIN);

        // single thread
        int[] counters = new int[size];
        for (int i = 0; i < len; i++) {
            ClickHouseNode node = nodes.apply(nodeSelector);
            counters[node.getPort() - 1] += 1;
        }

        for (int i = 0; i < size; i++) {
            if (tag == -1) {
                Assert.assertEquals(counters[i], requests);
            } else if (i == tag) {
                Assert.assertEquals(counters[i], len);
            } else {
                Assert.assertEquals(counters[i], 0);
            }
        }

        // multi-thread
        CountDownLatch latch = new CountDownLatch(len);
        List<ClickHouseNode> results = Collections.synchronizedList(new ArrayList<>(len));
        ExecutorService executor = Executors.newFixedThreadPool(3);
        for (int i = 0; i < len; i++) {
            executor.execute(() -> {
                ClickHouseNode node = nodes.apply(nodeSelector);
                results.add(node);
                latch.countDown();
            });
        }

        if (!latch.await(MAX_WAIT_SECONDS, TimeUnit.SECONDS)) {
            Assert.fail("Failed to complete operation within " + MAX_WAIT_SECONDS
                    + " seconds - check exception log to see what's going on");
        }
        counters = new int[size];
        for (int i = 0; i < len; i++) {
            counters[results.get(i).getPort() - 1] += 1;
        }
        for (int i = 0; i < size; i++) {
            if (tag == -1) {
                Assert.assertEquals(counters[i], requests);
            } else if (i == tag) {
                Assert.assertEquals(counters[i], len);
            } else {
                Assert.assertEquals(counters[i], 0);
            }
        }
    }

    @Test(dataProvider = "nodeSelectorProvider", groups = { "unit" })
    public void testRandom(ClickHouseNodeSelector nodeSelector) throws InterruptedException {
        int size = 5;
        int requests = 500;
        int len = size * requests;
        int tag = nodeSelector != null && nodeSelector.getPreferredTags().size() > 0
                ? Integer.parseInt(nodeSelector.getPreferredTags().iterator().next())
                : -1;

        ClickHouseNodes nodes = createNodes(size, ClickHouseLoadBalancingPolicy.RANDOM);

        // single thread
        int[] counters = new int[size];
        for (int i = 0; i < len; i++) {
            ClickHouseNode node = nodes.apply(nodeSelector);
            counters[node.getPort() - 1] += 1;
        }

        for (int i = 0; i < size; i++) {
            if (tag == -1) {
                Assert.assertTrue(counters[i] > 0, "All nodes should have been touched");
            } else if (i == tag) {
                Assert.assertEquals(counters[i], len);
            } else {
                Assert.assertEquals(counters[i], 0);
            }
        }

        // multi-thread
        CountDownLatch latch = new CountDownLatch(len);
        List<ClickHouseNode> results = Collections.synchronizedList(new ArrayList<>(len));
        ExecutorService executor = Executors.newFixedThreadPool(3);
        for (int i = 0; i < len; i++) {
            executor.execute(() -> {
                ClickHouseNode node = nodes.apply(nodeSelector);
                results.add(node);
                latch.countDown();
            });
        }

        if (!latch.await(MAX_WAIT_SECONDS, TimeUnit.SECONDS)) {
            Assert.fail("Failed to complete operation within " + MAX_WAIT_SECONDS
                    + " seconds - check exception log to see what's going on");
        }
        counters = new int[size];
        for (int i = 0; i < len; i++) {
            counters[results.get(i).getPort() - 1] += 1;
        }
        for (int i = 0; i < size; i++) {
            if (tag == -1) {
                Assert.assertTrue(counters[i] > 0, "All nodes should have been touched");
            } else if (i == tag) {
                Assert.assertEquals(counters[i], len);
            } else {
                Assert.assertEquals(counters[i], 0);
            }
        }
    }
}
