package com.clickhouse.jdbc.internal;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseLoadBalancingPolicy;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser.ConnectionInfo;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseJdbcUrlParserTest {
    @Test(groups = "unit")
    public void testParseInvalidUri() {
        Assert.assertThrows(SQLException.class, () -> ClickHouseJdbcUrlParser.parse(null, null));
        Assert.assertThrows(SQLException.class, () -> ClickHouseJdbcUrlParser.parse("", null));
        Assert.assertThrows(SQLException.class, () -> ClickHouseJdbcUrlParser.parse("some_invalid_uri", null));
        Assert.assertThrows(SQLException.class, () -> ClickHouseJdbcUrlParser.parse("jdbc:clickhouse:.", null));
        Assert.assertThrows(SQLException.class,
                () -> ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://", null));
        Assert.assertThrows(SQLException.class,
                () -> ClickHouseJdbcUrlParser.parse("jdbc:clickhouse:///db", null));
        Assert.assertThrows(SQLException.class,
                () -> ClickHouseJdbcUrlParser.parse("clickhouse://a:b:c@aaa", null));
        Assert.assertThrows(SQLException.class,
                () -> ClickHouseJdbcUrlParser.parse("clickhouse://::1:1234/a", null));
    }

    @Test(groups = "unit")
    public void testParseIpv6() throws SQLException, URISyntaxException {
        ConnectionInfo info = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://[::1]:1234", null);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:http://[::1]:1234"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("[::1]").port(ClickHouseProtocol.HTTP, 1234).build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://[::1]/", null);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:http://[::1]:8123"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("[::1]").port(ClickHouseProtocol.HTTP).build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://[::1]/dbdb", null);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:http://[::1]:8123/dbdb"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("[::1]").port(ClickHouseProtocol.HTTP).database("dbdb")
                        .build());
    }

    @Test(groups = "unit")
    public void testParseAbbrevation() throws SQLException, URISyntaxException {
        ConnectionInfo info = ClickHouseJdbcUrlParser.parse("jdbc:ch://localhost", null);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:http://localhost:8123"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.HTTP).build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:ch:grpc://localhost", null);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:grpc://localhost:9100"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.GRPC).build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:ch:https://:letmein@[::1]:3218/db1?user=aaa", null);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:http://[::1]:3218/db1?ssl=true&sslmode=STRICT"));
        Assert.assertEquals(info.getServer(), ClickHouseNode.builder().host("[::1]")
                .port(ClickHouseProtocol.HTTP, 3218)
                .database("db1")
                .credentials(ClickHouseCredentials.fromUserAndPassword("aaa", "letmein"))
                .addOption("ssl", "true").addOption("sslmode", "STRICT").build());
        Assert.assertEquals(info.getServer().getCredentials().orElse(null),
                ClickHouseCredentials.fromUserAndPassword("aaa", "letmein"));
    }

    @Test(groups = "unit")
    public void testParse() throws SQLException, URISyntaxException {
        ConnectionInfo info = ClickHouseJdbcUrlParser.parse("jdbc:ch://localhost", null);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:http://localhost:8123"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.HTTP).build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:ch:grpc://localhost/default", null);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:grpc://localhost:9100/default"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.GRPC)
                        .database((String) ClickHouseDefaults.DATABASE
                                .getEffectiveDefaultValue())
                        .build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:ch:https://:letmein@127.0.0.1:3218/db1", null);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:http://127.0.0.1:3218/db1?ssl=true&sslmode=STRICT"));
        Assert.assertEquals(info.getServer(), ClickHouseNode.builder().host("127.0.0.1")
                .port(ClickHouseProtocol.HTTP, 3218).database("db1")
                .credentials(ClickHouseCredentials
                        .fromUserAndPassword((String) ClickHouseDefaults.USER
                                .getEffectiveDefaultValue(), "letmein"))
                .addOption("ssl", "true").addOption("sslmode", "STRICT")
                .build());
    }

    @Test(groups = "unit")
    public void testParseWithProperties() throws SQLException, URISyntaxException {
        ConnectionInfo info = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://localhost/", null);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:http://localhost:8123"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.HTTP).build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://localhost:4321/ndb", null);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:http://localhost:4321/ndb"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.HTTP, 4321)
                        .database("ndb").build());

        Properties props = new Properties();
        props.setProperty("database", "db1");
        info = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://me@localhost:1234/mydb?password=123", props);
        Assert.assertEquals(info.getServer().toUri(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX),
                new URI("jdbc:clickhouse:http://localhost:1234/db1"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.HTTP, 1234)
                        .database("db1")
                        .credentials(ClickHouseCredentials.fromUserAndPassword("me", "123"))
                        .build());
        Assert.assertEquals(info.getProperties().getProperty("database"), "db1");
    }

    @Test(groups = "unit")
    public void testParseCredentials() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "default1");
        props.setProperty("password", "password1");
        ConnectionInfo connInfo = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://user:a:passwd@foo.ch/test",
                props);
        ClickHouseNode server = connInfo.getServer();
        Assert.assertEquals(connInfo.getDefaultCredentials().getUserName(), "default1");
        Assert.assertEquals(connInfo.getDefaultCredentials().getPassword(), "password1");
        Assert.assertEquals(server.getCredentials().get().getUserName(), "user");
        Assert.assertEquals(server.getCredentials().get().getPassword(), "a:passwd");

        server = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://let%40me%3Ain:let%40me%3Ain@foo.ch", null)
                .getServer();
        Assert.assertEquals(server.getCredentials().get().getUserName(), "let@me:in");
        Assert.assertEquals(server.getCredentials().get().getPassword(), "let@me:in");
    }

    @Test(groups = "unit", dataProvider = "testParseUrlPropertiesProvider")
    public void testParseUrlProperties(String url, int numOfNodes) throws SQLException {

        ConnectionInfo info = ClickHouseJdbcUrlParser.parse(url, null);
        Assert.assertEquals(info.getNodes().getNodes().size(), numOfNodes);
        Assert.assertEquals(info.getNodes().getPolicy().getClass().getSimpleName(), "FirstAlivePolicy");
        for (ClickHouseNode n : info.getNodes().getNodes()) {
            Assert.assertEquals(n.getOptions().get("connect_timeout"), "10000");
            Assert.assertEquals(n.getOptions().get("http_connection_provider"), "HTTP_CLIENT");
        }
    }

    @DataProvider(name = "testParseUrlPropertiesProvider")
    public static Object[][] testParseUrlPropertiesProvider() {
        return new Object[][] {
                { "jdbc:clickhouse://host1:8123,host2:8123,host3:8123/db1?http_connection_provider=HTTP_CLIENT&load_balancing_policy=firstAlive&connect_timeout=10000", 3 },
                { "jdbc:clickhouse:http://host1:8123,host2:8123,host3:8123/db1?http_connection_provider=HTTP_CLIENT&load_balancing_policy=firstAlive&connect_timeout=10000", 3 }
        };
    }
}
