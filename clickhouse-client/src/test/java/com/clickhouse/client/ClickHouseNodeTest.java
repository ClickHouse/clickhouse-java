package com.clickhouse.client;

import com.clickhouse.config.ClickHouseOption;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseVersion;

public class ClickHouseNodeTest extends BaseIntegrationTest {
    private void checkDefaultValues(ClickHouseNode node) {
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getDatabase().orElse(null), null);
        Assert.assertEquals(node.getProtocol(), ClickHouseDefaults.PROTOCOL.getEffectiveDefaultValue());
        Assert.assertFalse(node.getCredentials().isPresent());
        Assert.assertTrue(node.getTags().isEmpty());
        Assert.assertNotNull(node.getAddress());
        Assert.assertEquals(node.getHost(), ClickHouseDefaults.HOST.getEffectiveDefaultValue());
        Assert.assertEquals(node.getPort(), ClickHouseProtocol.ANY.getDefaultPort());
    }

    private void checkCustomValues(ClickHouseNode node, String cluster, String host, int port, int weight,
            ClickHouseProtocol protocol, String database, ClickHouseCredentials credentials,
            String[] tags) {
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

        ClickHouseNode node = ClickHouseNode.builder().cluster(cluster).host(host).port(protocol, port)
                .weight(weight)
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

        ClickHouseNode base = ClickHouseNode.builder().cluster(cluster).host(host).port(protocol, port)
                .weight(weight)
                .database(database).credentials(credentials).tags(null, tags).build();
        ClickHouseNode node = ClickHouseNode.builder(base).build();
        checkCustomValues(node, cluster, host, port, weight, protocol, database, credentials, tags);

        node = ClickHouseNode.builder(base).cluster(null).host(null).port(null, null).weight(null)
                .database(null)
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
        checkCustomValues(node, "", host, port, 1, protocol, database, null, new String[0]);

        protocol = ClickHouseProtocol.GRPC;
        node = ClickHouseNode.of(host, protocol, port, database, "read-only", "primary");
        checkCustomValues(node, "", host, port, 1, protocol, database, null, new String[] { "read-only", "primary" });
    }

    @Test(groups = { "unit" })
    public void testCredentials() {
        ClickHouseNode node = ClickHouseNode.builder().build();
        Assert.assertEquals(node.getCredentials().orElse(null), null);

        ClickHouseCredentials credentials = ClickHouseCredentials.fromUserAndPassword("default", "password");
        node = ClickHouseNode.builder().credentials(credentials).build();
        Assert.assertEquals(node.getCredentials().orElse(null), credentials);
        Assert.assertTrue(node.getCredentials().orElse(null) == credentials, "Should be the exact same instance");
    }

    @Test(groups = { "unit" })
    public void testDatabase() {
        ClickHouseConfig config = new ClickHouseConfig(
                Collections.singletonMap(ClickHouseClientOption.DATABASE, "ttt"),
                null, null, null);
        ClickHouseNode node = ClickHouseNode.builder().build();
        Assert.assertEquals(node.hasPreferredDatabase(), false);
        Assert.assertEquals(node.getDatabase().orElse(null), null);
        Assert.assertEquals(node.getDatabase(config), config.getDatabase());

        node = ClickHouseNode.builder().database("").build();
        Assert.assertEquals(node.hasPreferredDatabase(), false);
        // Assert.assertEquals(node.getDatabase().orElse(null), "");
        Assert.assertEquals(node.getDatabase(config), config.getDatabase());

        node = ClickHouseNode.builder().database("123").build();
        Assert.assertEquals(node.hasPreferredDatabase(), true);
        Assert.assertEquals(node.getDatabase().orElse(null), "123");
        Assert.assertEquals(node.getDatabase(config), "ttt");
        Assert.assertEquals(node.getDatabase(new ClickHouseConfig()), "123");
    }

    @Test(groups = { "unit" })
    public void testNullOrEmptyNodes() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNode.of((String) null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNode.of((URI) null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNode.of(""));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNode.of(" "));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNode.of("://"));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNode.of("://?"));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNode.of("://?&"));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNode.of("://?&#"));

        Assert.assertEquals(ClickHouseNode.of("/"),
                new ClickHouseNode("localhost", ClickHouseProtocol.ANY, 0, null, null, null));
        Assert.assertEquals(ClickHouseNode.of("?"),
                new ClickHouseNode("localhost", ClickHouseProtocol.ANY, 0, null, null, null));
        Assert.assertEquals(ClickHouseNode.of("#"),
                new ClickHouseNode("localhost", ClickHouseProtocol.ANY, 0, null, null, null));
        Assert.assertEquals(ClickHouseNode.of("/?"),
                new ClickHouseNode("localhost", ClickHouseProtocol.ANY, 0, null, null, null));
        Assert.assertEquals(ClickHouseNode.of("/#"),
                new ClickHouseNode("localhost", ClickHouseProtocol.ANY, 0, null, null, null));
        Assert.assertEquals(ClickHouseNode.of("?#"),
                new ClickHouseNode("localhost", ClickHouseProtocol.ANY, 0, null, null, null));
        Assert.assertEquals(ClickHouseNode.of("/?#"),
                new ClickHouseNode("localhost", ClickHouseProtocol.ANY, 0, null, null, null));
    }

    @Test(groups = { "unit" })
    public void testInvalidNodes() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNode.of("//a b"));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNode.of("/ /"));
    }

    @Test(groups = { "unit" })
    public void testValidNodes() {
        Map<String, String> options = new HashMap<>();
        options.put(ClickHouseClientOption.SSL.getKey(), "false");
        options.put(ClickHouseClientOption.SSL_MODE.getKey(), ClickHouseSslMode.STRICT.name());
        options.put(ClickHouseClientOption.DATABASE.getKey(), "db1");

        Set<String> tags = new HashSet<>();
        tags.add("dc2");

        Assert.assertEquals(ClickHouseNode.of("grpcs://node1:1919/db1?!ssl&#dc2"),
                new ClickHouseNode("node1", ClickHouseProtocol.GRPC, 1919, null, options, tags));
    }

    @Test(groups = { "unit" })
    public void testSecureNode() {
        Map<String, String> options = new HashMap<>();
        options.put(ClickHouseClientOption.SSL.getKey(), "true");
        options.put(ClickHouseClientOption.SSL_MODE.getKey(), ClickHouseSslMode.STRICT.name());
        options.put(ClickHouseClientOption.DATABASE.getKey(), "db1");

        Assert.assertEquals(ClickHouseNode.of("https://node1:443/db1"),
                new ClickHouseNode("node1", ClickHouseProtocol.HTTP, 443, null, options, null));
        Assert.assertEquals(ClickHouseNode.of("tcps://node1?database=db1"),
                new ClickHouseNode("node1", ClickHouseProtocol.TCP, 9440, null, options, null));
    }

    @Test(groups = { "unit" })
    public void testSingleWordNode() {
        for (String uri : new String[] {
                "noidea",
                "noidea/",
                "noidea?",
                "noidea#",
                "noidea?&",
                "noidea?&#",
                "noidea/?",
                "noidea/#",
                "noidea/?&",
                "noidea/?#",
                "noidea/?&#",
        }) {
            Assert.assertEquals(ClickHouseNode.of(uri),
                    new ClickHouseNode("noidea", ClickHouseProtocol.ANY, 0, null, null, null));
        }

        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseNode.of(new URI("noidea"), null));
    }

    @Test(groups = { "unit" })
    public void testNodeWithProtocol() {
        Map<String, String> options = new HashMap<>();
        options.put(ClickHouseClientOption.SSL.getKey(), "true");
        options.put(ClickHouseClientOption.SSL_MODE.getKey(), ClickHouseSslMode.STRICT.name());

        for (ClickHouseProtocol p : ClickHouseProtocol.values()) {
            Assert.assertEquals(ClickHouseNode.of(p.name() + ":///?#"),
                    new ClickHouseNode("localhost", p, p.getDefaultPort(), null, null, null));
            for (String s : p.getUriSchemes()) {
                boolean secure = p != ClickHouseProtocol.POSTGRESQL && s.endsWith("s");
                Assert.assertEquals(ClickHouseNode.of(s + ":///?#"),
                        new ClickHouseNode("localhost", p, secure
                                ? p.getDefaultSecurePort()
                                : p.getDefaultPort(),
                                null, secure ? options : null, null));
            }
        }
    }

    @Test(groups = { "unit" })
    public void testNodeWithHostAndPort() {
        Assert.assertEquals(ClickHouseNode.of(":-1"),
                new ClickHouseNode("localhost", ClickHouseProtocol.ANY, 0, null, null, null));
        Assert.assertEquals(ClickHouseNode.of(":8443"),
                new ClickHouseNode("localhost", ClickHouseProtocol.ANY, 0, null, null, null));
        Assert.assertEquals(ClickHouseNode.of("a:-1"),
                new ClickHouseNode("localhost", ClickHouseProtocol.ANY, 0, null, null, null));
        Assert.assertEquals(ClickHouseNode.of("a:0"),
                new ClickHouseNode("a", ClickHouseProtocol.ANY, 0, null, null, null));
        Assert.assertEquals(ClickHouseNode.of("a:65535"),
                new ClickHouseNode("a", ClickHouseProtocol.ANY, 65535, null, null, null));
        Assert.assertEquals(ClickHouseNode.of("a:65536"),
                new ClickHouseNode("a", ClickHouseProtocol.ANY, 0, null, null, null));
    }

    @Test(groups = { "unit" })
    public void testNodeWithDatabase() {
        Map<String, String> options = new HashMap<>();
        options.put(ClickHouseClientOption.SSL.getKey(), "true");
        options.put(ClickHouseClientOption.SSL_MODE.getKey(), ClickHouseSslMode.STRICT.name());

        Assert.assertEquals(ClickHouseNode.of("grpcs://node1:19100/"),
                new ClickHouseNode("node1", ClickHouseProtocol.GRPC, 19100, null, options, null));

        for (String uri : new String[] {
                "tcp://node1:9001/test",
                "tcp://node1:9001/test?",
                "tcp://node1:9001/test#",
                "tcp://node1:9001/test?#"
        }) {
            Assert.assertEquals(ClickHouseNode.of(uri),
                    new ClickHouseNode("node1", ClickHouseProtocol.TCP, 9001, null,
                            Collections.singletonMap(
                                    ClickHouseClientOption.DATABASE.getKey(),
                                    "test"),
                            null));
        }

        Assert.assertEquals(ClickHouseNode.of("tcp://node1:9001/test/"),
                new ClickHouseNode("node1", ClickHouseProtocol.TCP, 9001, null,
                        Collections.singletonMap(ClickHouseClientOption.DATABASE.getKey(),
                                "test/"),
                        null));

        String db = ClickHouseNode
                .of("localhost/testdb",
                        Collections.singletonMap(ClickHouseClientOption.DATABASE.getKey(),
                                "system"))
                .getDatabase().orElse(null);
        Assert.assertEquals(db, "system");
    }

    @Test(groups = { "unit" })
    public void testNodeWithCredentials() {
        Assert.assertEquals(ClickHouseNode.of("grpc://letmein@master"),
                new ClickHouseNode("master", ClickHouseProtocol.GRPC,
                        ClickHouseProtocol.GRPC.getDefaultPort(),
                        ClickHouseCredentials.fromUserAndPassword("letmein", ""), null, null));
        Assert.assertEquals(ClickHouseNode.of("grpc://letmein:test@master"),
                new ClickHouseNode("master", ClickHouseProtocol.GRPC,
                        ClickHouseProtocol.GRPC.getDefaultPort(),
                        ClickHouseCredentials.fromUserAndPassword("letmein", "test"), null,
                        null));
        Assert.assertEquals(ClickHouseNode.of("grpc://letmein:test@master/?password=secret"),
                new ClickHouseNode("master", ClickHouseProtocol.GRPC,
                        ClickHouseProtocol.GRPC.getDefaultPort(),
                        ClickHouseCredentials.fromUserAndPassword("letmein", "secret"), null,
                        null));
        Assert.assertEquals(ClickHouseNode.of("grpc://letmein:test@master/?user=me&password=secret"),
                new ClickHouseNode("master", ClickHouseProtocol.GRPC,
                        ClickHouseProtocol.GRPC.getDefaultPort(),
                        ClickHouseCredentials.fromUserAndPassword("me", "secret"), null,
                        null));

        Assert.assertEquals(
                ClickHouseNode.of("https://:letmein@[::1]:3218/db1?user=aaa").getCredentials()
                        .orElse(null),
                ClickHouseCredentials.fromUserAndPassword("aaa", "letmein"));
        Assert.assertEquals(ClickHouseNode.of("https://:letmein@[::1]:3218/db1").getCredentials().orElse(null),
                ClickHouseCredentials.fromUserAndPassword(
                        (String) ClickHouseDefaults.USER.getEffectiveDefaultValue(),
                        "letmein"));
    }

    @Test(groups = { "unit" })
    public void testNodeWithOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(ClickHouseClientOption.ASYNC.getKey(), "false");
        options.put(ClickHouseClientOption.SSL.getKey(), "true");
        options.put(ClickHouseClientOption.SSL_MODE.getKey(), ClickHouseSslMode.STRICT.name());
        options.put(ClickHouseClientOption.CONNECTION_TIMEOUT.getKey(), "500");

        for (String uri : new String[] {
                "https://node1?!async&ssl&connect_timeout=500",
                "http://node1?async=false&ssl=true&sslmode=STRICT&connect_timeout=500",
                "http://node1?&&&&async=false&ssl&&&&&sslmode=STRICT&connect_timeout=500&&&",
        }) {
            Assert.assertEquals(ClickHouseNode.of(uri),
                    new ClickHouseNode("node1", ClickHouseProtocol.HTTP,
                            ClickHouseProtocol.HTTP.getDefaultSecurePort(),
                            null, options, null));
        }
    }

    @Test(groups = { "unit" })
    public void testNodeWithTags() {
        Set<String> tags = new HashSet<>();
        tags.add("dc1");
        tags.add("r1s1");
        tags.add("read-only");

        for (String uri : new String[] {
                "tcp://node1#dc1,r1s1,read-only",
                "tcp://node1?#r1s1,dc1,read-only",
                "tcp://node1/#dc1,read-only,r1s1",
                "tcp://node1/#,,,,,,,dc1,,,,read-only,r1s1,,,,",
        }) {
            Assert.assertEquals(ClickHouseNode.of(uri),
                    new ClickHouseNode("node1", ClickHouseProtocol.TCP,
                            ClickHouseProtocol.TCP.getDefaultPort(), null,
                            null, tags));
        }
    }

    @Test(groups = { "unit" })
    public void testCaseInsensitiveEnumValue() {
        ClickHouseNode server = ClickHouseNode.of("localhost?format=rowbinary&sslmode=strict");
        Assert.assertEquals(server.config.getFormat(), ClickHouseFormat.RowBinary);
        Assert.assertEquals(server.config.getSslMode(), ClickHouseSslMode.STRICT);
    }

    @Test(groups = { "unit" })
    public void testQueryWithSlash() throws URISyntaxException {
        ClickHouseNode server = ClickHouseNode.of("http://localhost?a=/b/c/d");
        Assert.assertEquals(server.getDatabase().orElse(null), null);
        Assert.assertEquals(server.getOptions(), Collections.singletonMap("a", "/b/c/d"));
        Assert.assertEquals(server.toUri(), new URI("http://localhost:8123?a=/b/c/d"));

        server = ClickHouseNode.of("http://localhost:1234?/a/b/c=d");
        Assert.assertEquals(server.getDatabase().orElse(null), null);
        Assert.assertEquals(server.getOptions(), Collections.singletonMap("/a/b/c", "d"));
        Assert.assertEquals(server.toUri(), new URI("http://localhost:1234?/a/b/c=d"));

        Assert.assertEquals(ClickHouseNode.of("https://myserver/db/1/2/3?a%20=%201&b=/root/my.crt").toUri(),
                new URI("http://myserver:8443/db/1/2/3?a%20=%201&b=/root/my.crt&ssl=true&sslmode=STRICT"));
    }

    @Test(groups = { "integration" })
    public void testProbe() {
        // FIXME does not support ClickHouseProtocol.POSTGRESQL for now
        ClickHouseProtocol[] protocols = null;
        if ( isCloud() ) {
            protocols = new ClickHouseProtocol[]{
                    ClickHouseProtocol.HTTP
            };
        } else {
            protocols = new ClickHouseProtocol[]{ClickHouseProtocol.GRPC,
                    ClickHouseProtocol.HTTP, ClickHouseProtocol.MYSQL, ClickHouseProtocol.TCP};
        }
        ClickHouseVersion serverVersion = ClickHouseVersion
                .of(System.getProperty("clickhouseVersion", "latest"));
        for (ClickHouseProtocol p : protocols) {
            if (p == ClickHouseProtocol.GRPC && !serverVersion.check("[21.1,)")) {
                continue;
            }
            int port = p.getDefaultPort();
            if (isCloud()) {
                port = 8443;
            }
            ClickHouseNode node = getServer(ClickHouseProtocol.ANY, port);
            ClickHouseNode probedNode = node.probe();
            Assert.assertNotEquals(probedNode, node);
            Assert.assertEquals(probedNode.getProtocol(), p);
        }
    }

    @Test(groups = { "unit" })
    public void testToString() {
        Assert.assertTrue(ClickHouseNode.of("a?b=1").toString()
                .startsWith("ClickHouseNode [uri=any://a:0/default, options={b=1}]@"));
        Assert.assertTrue(ClickHouseNode.of("a?b=1&sslkey=secret").toString()
                .startsWith("ClickHouseNode [uri=any://a:0/default, options={b=1,sslkey=*}]@"));
        Assert.assertTrue(ClickHouseNode.of("a?password=*&b=1&sslkey=secret").toString()
                .startsWith("ClickHouseNode [uri=any://a:0/default, options={b=1,sslkey=*}]@"));
    }

    @Test(groups = { "unit" })
    public void testToUri() {
        Assert.assertEquals(ClickHouseNode.of("a?b=1").toUri().toString(), "any://a:0?b=1");
        Assert.assertEquals(ClickHouseNode.of("a/b/c?d=1").toUri().toString(), "any://a:0/b/c?d=1");
        Assert.assertEquals(
                ClickHouseNode.of("http://test:test@server1.dc1/db1?user=default&!async&auto_discovery#apj,r1s1")
                        .toUri().toString(),
                "http://server1.dc1:8123/db1?async=false&auto_discovery=true#apj,r1s1");
    }

    @Test(groups = { "unit" }, dataProvider = "testPropertyWithValueList_endpoints")
    public void testPropertyWithValueList(String endpoints, int numOfNodes, String[] expectedBaseUris) {
        ClickHouseNodes node = ClickHouseNodes.of(endpoints);
        Assert.assertEquals(node.nodes.size(), numOfNodes, "Number of nodes does not match");

        int i = 0;
        for (ClickHouseNode n : node.nodes) {
            Assert.assertEquals(n.config.getDatabase(), "my_db");
            Assert.assertEquals(expectedBaseUris[i++], n.getBaseUri());
            String customSettings = (String)n.config.getOption(ClickHouseClientOption.CUSTOM_SETTINGS);
            String configSettings = (String) n.config.getOption(ClickHouseClientOption.CUSTOM_SETTINGS);

            Arrays.asList(customSettings, configSettings).forEach((settings) -> {
                Map<String, String> settingsMap = ClickHouseOption.toKeyValuePairs(settings);
                Assert.assertEquals(settingsMap.get("param1"), "value1");
                Assert.assertEquals(settingsMap.get("param2"), "value2");
            });
        }
    }

    @DataProvider(name = "testPropertyWithValueList_endpoints")
    public static Object[][] endpoints() {
        return new Object[][] {
            { "http://server1:9090/my_db?custom_settings=param1=value1,param2=value2", 1, new String[]{"http://server1:9090/"} },
            { "http://server1/my_db?custom_settings=param1=value1,param2=value2", 1, new String[]{"http://server1:8123/"} },
            { "http://server1:9090,server2/my_db?custom_settings=param1=value1,param2=value2", 2, new String[]{"http://server1:9090/", "http://server2:8123/"} },
            { "http://server1,server2:9090/my_db?custom_settings=param1=value1,param2=value2", 2, new String[]{"http://server1:8123/", "http://server2:9090/"} }
        };
    }
}
