package com.clickhouse.client;

import java.util.ArrayList;
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

public class ClickHouseClusterTest extends BaseIntegrationTest {
    private ClickHouseCluster createCluster(int size) {
        ClickHouseNode template = ClickHouseNode.builder().host("test.host").build();

        ClickHouseNode[] nodes = new ClickHouseNode[size];
        for (int i = 0; i < size; i++) {
            nodes[i] = ClickHouseNode.builder(template).port(ClickHouseProtocol.HTTP, i + 1)
                    .tags(String.valueOf(i % size)).build();
        }

        return ClickHouseCluster.of(nodes);
    }

    @DataProvider(name = "nodeSelectorProvider")
    private Object[][] getNodeSelectors() {
        return new Object[][] { { null }, { ClickHouseNodeSelector.EMPTY },
                { ClickHouseNodeSelector.of(Collections.emptyList(), Collections.singleton("3")) } };
    }

    @Test(dataProvider = "nodeSelectorProvider", groups = { "unit" })
    public void testGetNode(ClickHouseNodeSelector nodeSelector) throws Exception {
        int size = 5;
        int requests = 500;
        int len = size * requests;
        int tag = nodeSelector != null && nodeSelector.getPreferredTags().size() > 0
                ? Integer.parseInt(nodeSelector.getPreferredTags().iterator().next())
                : -1;

        ClickHouseCluster cluster = createCluster(size);

        // single thread
        int[] counters = new int[size];
        for (int i = 0; i < len; i++) {
            ClickHouseNode node = cluster.apply(nodeSelector);
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
                ClickHouseNode node = cluster.apply(nodeSelector);
                results.add(node);
                latch.countDown();
            });
        }

        latch.await(15, TimeUnit.SECONDS);
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
    public void testCheck(ClickHouseNodeSelector nodeSelector) {
        int size = 5000;
        int len = nodeSelector != null && nodeSelector.getPreferredTags().size() > 0 ? 1 : size;

        ClickHouseCluster cluster = createCluster(size);
        for (int i = 0; i < len; i++) {
            ClickHouseNode node = cluster.apply(nodeSelector);
            node.updateStatus(Status.UNHEALTHY);
            Assert.assertTrue(cluster.getAvailableNodes().size() >= size - i - 1);
        }
    }

    @Test(groups = { "integration" })
    public void testProbe() {
        // FIXME does not support ClickHouseProtocol.POSTGRESQL for now
        ClickHouseProtocol[] protocols = new ClickHouseProtocol[] { ClickHouseProtocol.GRPC, ClickHouseProtocol.HTTP,
                ClickHouseProtocol.MYSQL, ClickHouseProtocol.NATIVE };

        for (ClickHouseProtocol p : protocols) {
            ClickHouseNode node = getServer(ClickHouseProtocol.ANY, p.getDefaultPort());
            ClickHouseNode probedNode = ClickHouseCluster.probe(node);
            Assert.assertNotEquals(probedNode, node);
            Assert.assertEquals(probedNode.getProtocol(), p);
        }
    }
}
