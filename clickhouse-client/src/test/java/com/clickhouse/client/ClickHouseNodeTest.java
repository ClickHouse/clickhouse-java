package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;

public class ClickHouseNodeTest {
    private void checkDefaultValues(ClickHouseNode node) {
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getCluster(), ClickHouseDefaults.CLUSTER.getEffectiveDefaultValue());
        Assert.assertEquals(node.getDatabase().orElse(null), null);
        Assert.assertEquals(node.getProtocol(), ClickHouseDefaults.PROTOCOL.getEffectiveDefaultValue());
        Assert.assertFalse(node.getCredentials().isPresent());
        Assert.assertTrue(node.getTags().isEmpty());
        Assert.assertNotNull(node.getAddress());
        Assert.assertEquals(node.getHost(), ClickHouseDefaults.HOST.getEffectiveDefaultValue());
        Assert.assertEquals(node.getPort(), ClickHouseDefaults.PORT.getEffectiveDefaultValue());
        Assert.assertEquals(node.getWeight(), ClickHouseDefaults.WEIGHT.getEffectiveDefaultValue());
    }

    private void checkCustomValues(ClickHouseNode node, String cluster, String host, int port, int weight,
            ClickHouseProtocol protocol, String database, ClickHouseCredentials credentials, String[] tags) {
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getCluster(), cluster);
        Assert.assertNotNull(node.getAddress());
        Assert.assertEquals(node.getHost(), host);
        Assert.assertEquals(node.getPort(), port);
        Assert.assertEquals(node.getWeight(), weight);
        Assert.assertEquals(node.getProtocol(), protocol);
        Assert.assertEquals(node.getDatabase().orElse(null), database);
        Assert.assertEquals(node.getCredentials().orElse(null), credentials);
        Assert.assertEquals(node.getTags().size(), tags.length);
        for (String t : tags) {
            Assert.assertTrue(node.getTags().contains(t));
        }
    }

    @Test(groups = { "unit" })
    public void testDefaultNode() {
        checkDefaultValues(ClickHouseNode.builder().build());
    }

    @Test(groups = { "unit" })
    public void testCustomNode() {
        String cluster = "my_cluster";
        String database = "my_db";
        String host = "non-existing.host";
        int port = 38123;
        int weight = -100;
        ClickHouseProtocol protocol = ClickHouseProtocol.HTTP;
        ClickHouseCredentials credentials = ClickHouseCredentials.fromUserAndPassword("user", "passwd");
        String[] tags = new String[] { "dc1", "rack1", "server1", "id1" };

        ClickHouseNode node = ClickHouseNode.builder().cluster(cluster).host(host).port(protocol, port).weight(weight)
                .database(database).credentials(credentials).tags(Arrays.asList(tags)).build();
        checkCustomValues(node, cluster, host, port, weight, protocol, database, credentials, tags);
    }

    @Test(groups = { "unit" })
    public void testBuildWithNode() {
        String cluster = "my_cluster";
        String database = "my_db";
        String host = "non-existing.host";
        int port = 38123;
        int weight = -100;
        ClickHouseProtocol protocol = ClickHouseProtocol.HTTP;
        ClickHouseCredentials credentials = ClickHouseCredentials.fromUserAndPassword("user", "passwd");
        String[] tags = new String[] { "dc1", "rack1", "server1", "id1" };

        ClickHouseNode base = ClickHouseNode.builder().cluster(cluster).host(host).port(protocol, port).weight(weight)
                .database(database).credentials(credentials).tags(null, tags).build();
        ClickHouseNode node = ClickHouseNode.builder(base).build();
        checkCustomValues(node, cluster, host, port, weight, protocol, database, credentials, tags);

        node = ClickHouseNode.builder(base).cluster(null).host(null).port(null, null).weight(null).database(null)
                .credentials(null).tags(null, (String[]) null).build();
        checkDefaultValues(node);
    }

    @Test(groups = { "unit" })
    public void testBuildInOneGo() {
        String host = "non-existing.host";
        String database = "my_db";
        ClickHouseProtocol protocol = ClickHouseProtocol.TCP;
        int port = 19000;
        ClickHouseNode node = ClickHouseNode.of(host, protocol, port, database);
        checkCustomValues(node, (String) ClickHouseDefaults.CLUSTER.getEffectiveDefaultValue(), host, port,
                (int) ClickHouseDefaults.WEIGHT.getEffectiveDefaultValue(), protocol, database, null, new String[0]);

        protocol = ClickHouseProtocol.GRPC;
        node = ClickHouseNode.of(host, protocol, port, database, "read-only", "primary");
        checkCustomValues(node, (String) ClickHouseDefaults.CLUSTER.getEffectiveDefaultValue(), host, port,
                (int) ClickHouseDefaults.WEIGHT.getEffectiveDefaultValue(), protocol, database, null,
                new String[] { "read-only", "primary" });
    }

    @Test(groups = { "unit" })
    public void testDatabase() {
        ClickHouseConfig config = new ClickHouseConfig(Collections.singletonMap(ClickHouseClientOption.DATABASE, "ttt"),
                null, null, null);
        ClickHouseNode node = ClickHouseNode.builder().build();
        Assert.assertEquals(node.hasPreferredDatabase(), false);
        Assert.assertEquals(node.getDatabase().orElse(null), null);
        Assert.assertEquals(node.getDatabase(config), config.getDatabase());

        node = ClickHouseNode.builder().database("").build();
        Assert.assertEquals(node.hasPreferredDatabase(), false);
        Assert.assertEquals(node.getDatabase().orElse(null), "");
        Assert.assertEquals(node.getDatabase(config), config.getDatabase());

        node = ClickHouseNode.builder().database("123").build();
        Assert.assertEquals(node.hasPreferredDatabase(), true);
        Assert.assertEquals(node.getDatabase().orElse(null), "123");
        Assert.assertEquals(node.getDatabase(config), "123");
    }
}
