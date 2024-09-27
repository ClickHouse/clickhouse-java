package com.clickhouse.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.clickhouse.client.ClickHouseNode.Status;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseSslMode;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseNodesTest {
    @Test(groups = { "unit" })
    public void testNullOrEmptyList() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNodes.of(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNodes.of(""));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNodes.of(" "));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNodes.of(","));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNodes.of(",, , "));
    }

    @Test(groups = { "unit" })
    public void testBuildCacheKey() {
        String baseUri = "localhost";
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, null), baseUri);
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, new TreeMap<Object, Object>()), baseUri);
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, new Properties()), baseUri);

        Map<Object, Object> defaultOptions = new HashMap<>();
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, defaultOptions), baseUri);
        defaultOptions.put("b", " ");
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, defaultOptions), baseUri + "|b= ,");
        defaultOptions.put("a", 1);
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, defaultOptions), baseUri + "|a=1,b= ,");
        defaultOptions.put(" ", false);
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, defaultOptions),
                baseUri + "| =false,a=1,b= ,");
        defaultOptions.put(null, "null-key");
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, defaultOptions),
                baseUri + "| =false,a=1,b= ,");
        defaultOptions.put("null-value", null);
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, defaultOptions),
                baseUri + "| =false,a=1,b= ,null-value=null,");
        defaultOptions.put(null, null);
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, defaultOptions),
                baseUri + "| =false,a=1,b= ,null-value=null,");
        defaultOptions.put(ClickHouseDefaults.USER.getKey(), "hello ");
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, defaultOptions),
                baseUri + "| =false,a=1,b= ,null-value=null,user=hello ,");
        defaultOptions.put(ClickHouseDefaults.PASSWORD.getKey(), " /?&#");
        Assert.assertEquals(ClickHouseNodes.buildCacheKey(baseUri, defaultOptions),
                baseUri + "| =false,a=1,b= ,null-value=null,password= /?&#,user=hello ,");
        Assert.assertTrue(
                ClickHouseNodes.of(baseUri, defaultOptions) == ClickHouseNodes.of(baseUri,
                        defaultOptions),
                "Should be exact same node list");

        ClickHouseNodes nodes = ClickHouseNodes.of(baseUri, defaultOptions);
        String user = (String) defaultOptions.remove(ClickHouseDefaults.USER.getKey());
        String passwd = (String) defaultOptions.remove(ClickHouseDefaults.PASSWORD.getKey());
        Assert.assertEquals(nodes.getTemplate().getCredentials().orElse(null),
                ClickHouseCredentials.fromUserAndPassword(user, passwd));
        Assert.assertFalse(nodes.getTemplate().getOptions().containsKey(null), "Should not have null key");
        Assert.assertFalse(nodes.getTemplate().getOptions().containsKey("null-value"),
                "Should not have null value");
        Assert.assertFalse(nodes.getTemplate().getOptions().containsKey(ClickHouseDefaults.USER.getKey()),
                "Should not have user name");
        Assert.assertFalse(nodes.getTemplate().getOptions().containsKey(ClickHouseDefaults.PASSWORD.getKey()),
                "Should not have password");
        Assert.assertEquals(nodes.getTemplate().getOptions().get("a"), "1");
        Assert.assertEquals(nodes.getTemplate().getOptions().get(" "), "false");
    }

    @Test(groups = { "unit" })
    public void testCache() {
        Assert.assertTrue(ClickHouseNodes.of("a") == ClickHouseNodes.of("a"),
                "Should be the exact same node list");
        Assert.assertTrue(ClickHouseNodes.of("a,b,c") == ClickHouseNodes.of("a,b,c"),
                "Should be the exact same node list");
        Assert.assertTrue(ClickHouseNodes.of(" a") == ClickHouseNodes.of("a"),
                "Should be the exact same node list");
        Assert.assertTrue(ClickHouseNodes.of(" a,b,c ") == ClickHouseNodes.of("a,b,c"),
                "Should be the exact same node list");

        Assert.assertFalse(ClickHouseNodes.of("a, b,c") == ClickHouseNodes.of("a,b,c"),
                "Should be the exact same node list");
    }

    @Test(groups = { "unit" })
    public void testCredentials() {
        ClickHouseNodes nodes = ClickHouseNodes
                .of("https://dba:managed@node1,(node2),(tcp://aaa:bbb@node3)/test");
        Assert.assertEquals(nodes.getTemplate().getCredentials().orElse(null), null);
        Assert.assertEquals(nodes.nodes.get(0).getCredentials().orElse(null),
                ClickHouseCredentials.fromUserAndPassword("dba", "managed"));
        Map<String, String> options = new HashMap<>();
        options.put(ClickHouseDefaults.USER.getKey(), "");
        options.put(ClickHouseDefaults.PASSWORD.getKey(), "");
        Assert.assertEquals(ClickHouseNodes
                .of("https://dba:managed@node1,(node2),(tcp://aaa:bbb@node3)/test", options)
                .getTemplate().getCredentials().orElse(null), null);
        options.put(ClickHouseDefaults.USER.getKey(), "/u:s?e#r");
        options.put(ClickHouseDefaults.PASSWORD.getKey(), "");
        nodes = ClickHouseNodes.of("https://dba:managed@node1,(node2),(tcp://aaa:bbb@node3)/test", options);
        Assert.assertEquals(nodes.getTemplate().getCredentials().orElse(null),
                ClickHouseCredentials.fromUserAndPassword("/u:s?e#r", ""));
        Assert.assertEquals(nodes.nodes.get(0).getCredentials().orElse(null),
                ClickHouseCredentials.fromUserAndPassword("dba", "managed"));
        Assert.assertEquals(nodes.nodes.get(1).getCredentials().orElse(null),
                ClickHouseCredentials.fromUserAndPassword("/u:s?e#r", ""));
        Assert.assertEquals(nodes.nodes.get(2).getCredentials().orElse(null),
                ClickHouseCredentials.fromUserAndPassword("aaa", "bbb"));
        Assert.assertEquals(
                ClickHouseNodes.of("https://:letmein@[::1]:3218/db1?user=aaa").nodes.get(0)
                        .getCredentials().orElse(null),
                ClickHouseCredentials.fromUserAndPassword("aaa", "letmein"));
        Assert.assertEquals(
                ClickHouseNodes.of("https://aaa@[::1]:3218/db1?password=ppp").nodes.get(0)
                        .getCredentials().orElse(null),
                ClickHouseCredentials.fromUserAndPassword("aaa", "ppp"));
        Assert.assertEquals(
                ClickHouseNodes.of("https://[::1]:3218/db1?password=ppp").nodes.get(0)
                        .getCredentials().orElse(null),
                ClickHouseCredentials.fromUserAndPassword(
                        (String) ClickHouseDefaults.USER.getEffectiveDefaultValue(),
                        "ppp"));
    }

    @Test(groups = { "unit" })
    public void testFactoryMethods() {
        Properties props = new Properties();
        props.setProperty("database", "cc");
        props.setProperty("socket_timeout", "12345");
        props.setProperty("failover", "7");
        props.setProperty("load_balancing_policy", "roundRobin");
        for (ClickHouseNodes nodes : new ClickHouseNodes[] {
                ClickHouseNodes.of(
                        "http://host1,host2,host3/bb?database=cc&socket_timeout=12345&failover=7&load_balancing_policy=roundRobin"),
                ClickHouseNodes.of(
                        "http://host1,host2,host3?database=aa&socket_timeout=54321&failover=3&load_balancing_policy=random",
                        props),
                ClickHouseNodes.of("http://host1,host2,host3/bb", props)
        }) {
            Assert.assertEquals(nodes.template.config.getDatabase(), "cc");
            Assert.assertEquals(nodes.template.config.getSocketTimeout(), 12345);
            Assert.assertEquals(nodes.template.config.getFailover(), 7);
            Assert.assertEquals(nodes.template.config.getOption(ClickHouseClientOption.LOAD_BALANCING_POLICY),
                    ClickHouseLoadBalancingPolicy.ROUND_ROBIN);
        }
    }

    @Test(groups = { "unit" })
    public void testGetNodes() {
        // without selector
        ClickHouseNodes nodes = ClickHouseNodes.of("http://(node1#dc1),{node2#dc1},(node3#dc2)");
        Assert.assertEquals(nodes.getNodeSelector(), ClickHouseNodeSelector.EMPTY);
        Assert.assertEquals(nodes.getNodes(), Arrays.asList(ClickHouseNode.of("http://node1#dc1"),
                ClickHouseNode.of("http://node2#dc1"), ClickHouseNode.of("http://node3#dc2")));
        Assert.assertEquals(nodes.getNodes(ClickHouseNodeSelector.of("dc1"), 1),
                Arrays.asList(ClickHouseNode.of("http://node1#dc1")));
        Assert.assertEquals(nodes.getNodes(ClickHouseNodeSelector.of("dc1"), -1),
                Arrays.asList(ClickHouseNode.of("http://node1#dc1"),
                        ClickHouseNode.of("http://node2#dc1")));

        // with selector
        nodes = ClickHouseNodes
                .of("http://(node1#dc1),{node2#dc1},(node3#dc2)/?load_balancing_tags=dc1");
        Assert.assertEquals(nodes.getNodeSelector().getPreferredTags(), Collections.singleton("dc1"));
        Assert.assertEquals(nodes.getNodes(),
                Arrays.asList(ClickHouseNode.of("http://node1#dc1"),
                        ClickHouseNode.of("http://node2#dc1")));
        Assert.assertEquals(nodes.getNodes(ClickHouseNodeSelector.of("dc2"), 1),
                Arrays.asList(ClickHouseNode.of("http://node3#dc2")));
        Assert.assertEquals(nodes.getNodes(ClickHouseNodeSelector.of("dc2"), -1),
                Arrays.asList(ClickHouseNode.of("http://node3#dc2")));
    }

    @Test(groups = { "unit" })
    public void testNodeGrouping() throws ExecutionException, InterruptedException, TimeoutException {
        ClickHouseNodes nodes = ClickHouseNodes
                .of("http://(a?node_group_size=1),(tcp://b?x=1)/test?x=2&node_group_size=0");
        Assert.assertTrue(nodes.getPolicy() == ClickHouseLoadBalancingPolicy.DEFAULT,
                "Should be the default policy");
        Assert.assertEquals(nodes.getTemplate().config.getOption(ClickHouseClientOption.NODE_GROUP_SIZE), 0);
        Assert.assertEquals(nodes.getNodes().size(), 2);
        Assert.assertTrue(nodes.getFaultyNodes().isEmpty(), "Should NOT have any faulty node");
        Assert.assertEquals(nodes.getNodes().get(0), ClickHouseNode.of("http://a/test?x=2"));

        nodes = ClickHouseNodes.of(
                "http://(a?node_group_size=2),(tcp://b?x=1), (c)/test?x=2&node_group_size=1&load_balancing_policy=firstAlive&check_all_nodes=true");
        Assert.assertEquals(nodes.getTemplate().config.getOption(ClickHouseClientOption.CHECK_ALL_NODES), true);
        // make sure the one-time health check completed first
        Optional<ScheduledFuture<?>> o = nodes.scheduleHealthCheck();
        if (o.isPresent()) {
            o.get().get(nodes.getTemplate().config.getConnectionTimeout() * 2, TimeUnit.MILLISECONDS);
        }
        Assert.assertEquals(nodes.getTemplate().config.getOption(ClickHouseClientOption.NODE_GROUP_SIZE), 1);
        Assert.assertEquals(nodes.nodes.size(), 3);
        Assert.assertEquals(nodes.getNodes().size(), 1);
        Assert.assertEquals(nodes.faultyNodes.size(), 1);
        Assert.assertEquals(nodes.getFaultyNodes().size(), 1);
        Assert.assertEquals(nodes.getFaultyNodes().get(0), ClickHouseNode.of("http://a/test?x=2"));
        Assert.assertEquals(nodes.get(), ClickHouseNode.of("tcp://b:9000/test?x=1"));
    }

    @Test(groups = { "unit" }, enabled = false)
    public void testQueryWithSlash() {
        // test is disabled because this format of urls is not supported
        ClickHouseNodes servers = ClickHouseNodes
                .of("https://node1?a=/b/c/d,node2/db2?/a/b/c=d,node3/db1?a=/d/c.b");
        Assert.assertEquals(servers.nodes.get(0).getDatabase().orElse(null), "db1");
        Assert.assertEquals(servers.nodes.get(0).getOptions().get("a"), "/b/c/d");
        Assert.assertEquals(servers.nodes.get(1).getDatabase().orElse(null), "db2");
        Assert.assertEquals(servers.nodes.get(1).getOptions().get("a"), "/d/c.b");
        Assert.assertEquals(servers.nodes.get(1).getOptions().get("/a/b/c"), "d");
    }

    @Test(groups = { "unit" })
    public void testSingleNodeList() {
        String uri = "a";
        Assert.assertEquals(ClickHouseNodes.of(uri),
                new ClickHouseNodes(Arrays.asList(ClickHouseNode.of(uri))));
        Assert.assertEquals(ClickHouseNodes.of(uri = "http://a"),
                new ClickHouseNodes(Arrays.asList(ClickHouseNode.of(uri))));
        Assert.assertEquals(ClickHouseNodes.of(uri).get(), ClickHouseNode.of(uri));
        Assert.assertEquals(ClickHouseNodes.of(uri).apply(null), ClickHouseNode.of(uri));
        Assert.assertEquals(ClickHouseNodes.of(uri).apply(ClickHouseNodeSelector.EMPTY),
                ClickHouseNode.of(uri));

        Assert.assertEquals(ClickHouseNodes.of("http://(a?a=1#b,c,d)"),
                new ClickHouseNodes(Arrays.asList(ClickHouseNode.of("http://a?a=1#b,c,d"))));
        Assert.assertEquals(ClickHouseNodes.of("http://(a?a=1#b,c,d)/db?a=2&b=1"),
                new ClickHouseNodes(Arrays.asList(ClickHouseNode.of("http://a/db?a=1&b=1#b,c,d"))));

        Map<String, String> options = new HashMap<>();
        options.put(ClickHouseClientOption.SSL.getKey(), "true");
        options.put(ClickHouseClientOption.SSL_MODE.getKey(), ClickHouseSslMode.STRICT.name());
        options.put(ClickHouseClientOption.DATABASE.getKey(), "db1");

        Assert.assertEquals(ClickHouseNodes.of("https://node1:443/db1").nodes.get(0),
                new ClickHouseNode("node1", ClickHouseProtocol.HTTP, 443, null, options, null));
        Assert.assertEquals(ClickHouseNodes.of("tcps://node1?database=db1").nodes.get(0),
                new ClickHouseNode("node1", ClickHouseProtocol.TCP, 9440, null, options, null));
    }

    @Test(groups = { "unit" })
    public void testMultiNodeList() {
        Assert.assertEquals(ClickHouseNodes.of("a,b,c, b"), new ClickHouseNodes(
                Arrays.asList(ClickHouseNode.of("a"), ClickHouseNode.of("b"), ClickHouseNode.of("c"))));
        Assert.assertEquals(ClickHouseNodes.of(" ,,,,,a,, b ,c,,a, c,,, ,"), new ClickHouseNodes(
                Arrays.asList(ClickHouseNode.of("a"), ClickHouseNode.of("b"), ClickHouseNode.of("c"))));
        Assert.assertEquals(ClickHouseNodes.of("(a),[::1],{c}"), new ClickHouseNodes(
                Arrays.asList(ClickHouseNode.of("a"), ClickHouseNode.of("[::1]"),
                        ClickHouseNode.of("c"))));
        Assert.assertEquals(ClickHouseNodes.of("http://a,b,c"), new ClickHouseNodes(
                Arrays.asList(ClickHouseNode.of("http://a"), ClickHouseNode.of("http://b"),
                        ClickHouseNode.of("http://c"))));
        Assert.assertEquals(ClickHouseNodes.of("http://(a) , {b}, [::1]"), new ClickHouseNodes(
                Arrays.asList(ClickHouseNode.of("http://a"), ClickHouseNode.of("http://b"),
                        ClickHouseNode.of("http://[::1]"))));

        // THIS IS SHOULD NOT BE SUPPORTED
//        Assert.assertEquals(ClickHouseNodes.of("http://a,tcp://b,grpc://c"), new ClickHouseNodes(
//                Arrays.asList(ClickHouseNode.of("http://a"), ClickHouseNode.of("tcp://b"),
//                        ClickHouseNode.of("grpc://c"))));
//        Assert.assertEquals(ClickHouseNodes.of("http://a,tcp://b,grpc://c/"), new ClickHouseNodes(
//                Arrays.asList(ClickHouseNode.of("http://a"), ClickHouseNode.of("tcp://b"),
//                        ClickHouseNode.of("grpc://c"))));
//        Assert.assertEquals(ClickHouseNodes.of("http://a,tcp://b,grpc://c/db1"), new ClickHouseNodes(
//                Arrays.asList(ClickHouseNode.of("http://a/db1"), ClickHouseNode.of("tcp://b/db1"),
//                        ClickHouseNode.of("grpc://c/db1"))));
//        Assert.assertEquals(ClickHouseNodes.of("http://a,tcp://b,grpc://c?a=1"), new ClickHouseNodes(
//                Arrays.asList(ClickHouseNode.of("http://a?a=1"), ClickHouseNode.of("tcp://b?a=1"),
//                        ClickHouseNode.of("grpc://c?a=1"))));
//        Assert.assertEquals(ClickHouseNodes.of("http://a,tcp://b,grpc://c#dc1"), new ClickHouseNodes(
//                Arrays.asList(ClickHouseNode.of("http://a#dc1"), ClickHouseNode.of("tcp://b#dc1"),
//                        ClickHouseNode.of("grpc://c#dc1"))));
//        Assert.assertEquals(ClickHouseNodes.of("http://a,tcp://b,grpc://c:1234/some/db?a=1#dc1"),
//                new ClickHouseNodes(
//                        Arrays.asList(ClickHouseNode.of("http://a/some/db?a=1#dc1"),
//                                ClickHouseNode.of("tcp://b/some/db?a=1#dc1"),
//                                ClickHouseNode.of("grpc://c:1234/some/db?a=1#dc1"))));
    }

    @Test(groups = { "unit" }, enabled = false)
    public void testManageAndUnmanageNewNode() {
        // test is disabled because this format of urls is not supported
        ClickHouseNodes nodes = ClickHouseNodes.create("https://a,grpcs://b,mysql://c", null);
        Assert.assertEquals(nodes.getPolicy(), ClickHouseLoadBalancingPolicy.DEFAULT);
        Assert.assertEquals(nodes.nodes.size(), 3);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);
        ClickHouseNode node = ClickHouseNode.of("tcps://d");
        Assert.assertTrue(node.isStandalone(), "Newly created node is always standalone");
        nodes.update(node, Status.MANAGED);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 4);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);
        nodes.update(node, Status.STANDALONE);
        Assert.assertTrue(node.isStandalone(), "Removed node is always standalone");
        Assert.assertEquals(nodes.nodes.size(), 3);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);

        // now repeat same scenario but using different method
        node = ClickHouseNode.of("postgres://e");
        Assert.assertTrue(node.isStandalone(), "Newly created node is always standalone");
        node.setManager(nodes);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 4);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);
        node.setManager(null);
        Assert.assertTrue(node.isStandalone(), "Removed node is always standalone");
        Assert.assertEquals(nodes.nodes.size(), 3);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);
    }

    @Test(groups = { "unit" })
    public void testManageAndUnmanageSameNode() {
        ClickHouseNodes nodes = ClickHouseNodes.create("http://a,b,c", null);
        Assert.assertEquals(nodes.nodes.size(), 3);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);
        ClickHouseNode node = ClickHouseNode.of("http://a");
        Assert.assertTrue(node.isStandalone(), "Newly created node is always standalone");
        nodes.update(node, Status.MANAGED);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 3);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);
        nodes.update(node, Status.STANDALONE);
        Assert.assertTrue(node.isStandalone(), "Removed node is always standalone");
        Assert.assertEquals(nodes.nodes.size(), 2);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);

        // now repeat same scenario but using different method
        node = ClickHouseNode.of("http://b");
        Assert.assertTrue(node.isStandalone(), "Newly created node is always standalone");
        node.setManager(nodes);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 2);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);
        node.setManager(null);
        Assert.assertTrue(node.isStandalone(), "Removed node is always standalone");
        Assert.assertEquals(nodes.nodes.size(), 1);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);
    }

    @Test(groups = { "unit" })
    public void testManageAndUnmanageExistingNode() {
        ClickHouseNode nodeA = ClickHouseNode.of("a");
        ClickHouseNode nodeB = ClickHouseNode.of("b");
        ClickHouseNode nodeC = ClickHouseNode.of("c");
        ClickHouseNodes nodes = new ClickHouseNodes(Arrays.asList(nodeA, nodeB, nodeC));
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 3);
        ClickHouseNode node = nodeA;
        Assert.assertTrue(node.isManaged(), "Existing node should be managed");
        nodes.update(node, Status.MANAGED);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 3);
        nodes.update(node, Status.STANDALONE);
        Assert.assertTrue(node.isStandalone(), "Unmanaged node should NOT have status manager");
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 2);

        // now repeat same scenario but using different method
        node = nodeC;
        Assert.assertTrue(node.isManaged(), "Existing node should be managed");
        node.setManager(nodes);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 2);
        node.setManager(null);
        Assert.assertTrue(node.isStandalone(), "Unmanaged node should NOT have status manager");
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 1);
    }

    @Test(groups = { "unit" })
    public void testChangeStatusOfNewNode() {
        ClickHouseNodes nodes = ClickHouseNodes.create("a,b,c", null);
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 3);

        ClickHouseNode node = ClickHouseNode.of("x");
        Assert.assertTrue(node.isStandalone(), "Newly created node is always standalone");
        nodes.update(node, Status.HEALTHY);
        Assert.assertEquals(nodes.nodes.size(), 1);
        Assert.assertEquals(nodes.faultyNodes.size(), 3);
        nodes.update(node, Status.FAULTY);
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 4);

        // since the node is NOT managed, its update function won't work
        for (Status s : new Status[] {
                Status.HEALTHY,
                Status.FAULTY,
                Status.MANAGED,
                Status.STANDALONE
        }) {
            node.update(s);
            Assert.assertTrue(node.isStandalone(), "Newly created node is always standalone");
            Assert.assertEquals(nodes.nodes.size(), 0);
            Assert.assertEquals(nodes.faultyNodes.size(), 4);
        }

        // mark it as managed
        node.setManager(nodes);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 4);
        node.update(Status.STANDALONE);
        Assert.assertTrue(node.isStandalone(), "Removed node is always standalone");
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 3);

        node.setManager(nodes);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 4);
        Assert.assertEquals(nodes.faultyNodes.getLast(), node);
        node.update(Status.HEALTHY);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 1);
        Assert.assertEquals(nodes.faultyNodes.size(), 3);
        Assert.assertEquals(nodes.nodes.getFirst(), node);
    }

    @Test(groups = { "unit" })
    public void testChangeStatusOfSameNode() {
        ClickHouseNodes nodes = ClickHouseNodes.create("a,b,c", null);
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 3);

        ClickHouseNode node = ClickHouseNode.of("c");
        Assert.assertTrue(node.isStandalone(), "Newly created node is always standalone");
        nodes.update(node, Status.HEALTHY);
        Assert.assertEquals(nodes.nodes.size(), 1);
        Assert.assertEquals(nodes.faultyNodes.size(), 2);
        nodes.update(node, Status.FAULTY);
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 3);

        // since the node is NOT managed, its update function won't work
        for (Status s : new Status[] {
                Status.HEALTHY,
                Status.FAULTY,
                Status.MANAGED,
                Status.STANDALONE
        }) {
            node.update(s);
            Assert.assertTrue(node.isStandalone(), "Newly created node is always standalone");
            Assert.assertEquals(nodes.nodes.size(), 0);
            Assert.assertEquals(nodes.faultyNodes.size(), 3);
        }

        // mark it as managed
        node.setManager(nodes);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 3);
        node.update(Status.STANDALONE);
        Assert.assertTrue(node.isStandalone(), "Removed node is always standalone");
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 2);

        node.setManager(nodes);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 3);
        Assert.assertEquals(nodes.faultyNodes.getLast(), node);
        node.update(Status.HEALTHY);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 1);
        Assert.assertEquals(nodes.faultyNodes.size(), 2);
        Assert.assertEquals(nodes.nodes.getFirst(), node);
    }

    @Test(groups = { "unit" })
    public void testChangeStatusOfExistingNode() {
        ClickHouseNode nodeA = ClickHouseNode.of("http://a");
        ClickHouseNode nodeB = ClickHouseNode.of("tcp://b");
        ClickHouseNode nodeC = ClickHouseNode.of("grpc://c");
        ClickHouseNodes nodes = new ClickHouseNodes(Arrays.asList(nodeA, nodeB, nodeC));
        Assert.assertEquals(nodes.nodes.size(), 3);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);

        ClickHouseNode node = nodeA;
        Assert.assertTrue(node.isManaged(), "Existing node should be managed");
        nodes.update(node, Status.HEALTHY);
        Assert.assertEquals(nodes.nodes.size(), 3);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);
        nodes.update(node, Status.FAULTY);
        Assert.assertEquals(nodes.nodes.size(), 2);
        Assert.assertEquals(nodes.faultyNodes.size(), 1);

        // since the node is managed, mark it as managed
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        node.update(Status.HEALTHY);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 3);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);
        Assert.assertEquals(nodes.nodes.getLast(), node);
        node.update(Status.FAULTY);
        Assert.assertTrue(node.isManaged(), "Node should be managed");
        Assert.assertEquals(nodes.nodes.size(), 2);
        Assert.assertEquals(nodes.faultyNodes.size(), 1);
        Assert.assertEquals(nodes.faultyNodes.getLast(), node);

        // remove the node from the list
        node.setManager(null);
        for (Status s : new Status[] {
                Status.HEALTHY,
                Status.FAULTY,
                Status.MANAGED,
                Status.STANDALONE
        }) {
            node.update(s);
            Assert.assertTrue(node.isStandalone(), "Removed node is always standalone");
            Assert.assertEquals(nodes.nodes.size(), 2);
            Assert.assertEquals(nodes.faultyNodes.size(), 0);
        }
    }

    @Test(groups = { "unit" })
    public void testHealthCheck() {
        Map<String, String> options = new HashMap<>();
        options.put(ClickHouseClientOption.CHECK_ALL_NODES.getKey(), "true");
        ClickHouseNodes nodes = ClickHouseNodes.of("http://a,http://b", options);
        Assert.assertEquals(nodes.nodes.size(), 2);
        Assert.assertEquals(nodes.faultyNodes.size(), 0);

        nodes.check();
        Assert.assertEquals(nodes.nodes.size(), 0);
        Assert.assertEquals(nodes.faultyNodes.size(), 2);
    }
}
