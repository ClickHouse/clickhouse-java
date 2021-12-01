package com.clickhouse.jdbc.internal;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser.ConnectionInfo;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseJdbcUrlParserTest {
    @Test(groups = "unit")
    public void testRemoveCredentialsFromQuery() {
        Assert.assertEquals(ClickHouseJdbcUrlParser.removeCredentialsFromQuery(null), null);
        Assert.assertEquals(ClickHouseJdbcUrlParser.removeCredentialsFromQuery(""), null);
        Assert.assertEquals(ClickHouseJdbcUrlParser.removeCredentialsFromQuery(" "), null);
        Assert.assertEquals(ClickHouseJdbcUrlParser.removeCredentialsFromQuery("&"), null);
        Assert.assertEquals(ClickHouseJdbcUrlParser.removeCredentialsFromQuery(" & "), null);
        Assert.assertEquals(ClickHouseJdbcUrlParser.removeCredentialsFromQuery("a=1&b=2"), "a=1&b=2");
        Assert.assertEquals(ClickHouseJdbcUrlParser.removeCredentialsFromQuery("user=a"), null);
        Assert.assertEquals(ClickHouseJdbcUrlParser.removeCredentialsFromQuery("password=a%20b"), null);
        Assert.assertEquals(ClickHouseJdbcUrlParser.removeCredentialsFromQuery("user=default&password=a%20b"), null);
        Assert.assertEquals(ClickHouseJdbcUrlParser.removeCredentialsFromQuery("user=default&a=1&password=a%20b"),
                "a=1");
    }

    @Test(groups = "unit")
    public void testParseInvalidUri() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseJdbcUrlParser.parse(null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseJdbcUrlParser.parse("", null));
        Assert.assertThrows(URISyntaxException.class, () -> ClickHouseJdbcUrlParser.parse("some_invalid_uri", null));
        Assert.assertThrows(URISyntaxException.class, () -> ClickHouseJdbcUrlParser.parse("jdbc:clickhouse:.", null));
        Assert.assertThrows(URISyntaxException.class, () -> ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://", null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseJdbcUrlParser.parse("jdbc:clickhouse:///db", null));
        Assert.assertThrows(URISyntaxException.class,
                () -> ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://server/ ", null));
        Assert.assertThrows(URISyntaxException.class,
                () -> ClickHouseJdbcUrlParser.parse("clickhouse://a:b:c@aaa", null));
        Assert.assertThrows(URISyntaxException.class,
                () -> ClickHouseJdbcUrlParser.parse("clickhouse://::1:1234/a", null));
    }

    @Test(groups = "unit")
    public void testParseIpv6() throws URISyntaxException {
        ConnectionInfo info = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://[::1]:1234", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:clickhouse:http://[::1]:1234/default"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("[::1]").port(ClickHouseProtocol.HTTP, 1234)
                        .database((String) ClickHouseDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ClickHouseCredentials.fromUserAndPassword(
                                (String) ClickHouseDefaults.USER.getEffectiveDefaultValue(),
                                (String) ClickHouseDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());
    }

    @Test(groups = "unit")
    public void testParseAbbrevation() throws URISyntaxException {
        ConnectionInfo info = ClickHouseJdbcUrlParser.parse("jdbc:ch://localhost", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:clickhouse:http://localhost:8123/default"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.HTTP)
                        .database((String) ClickHouseDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ClickHouseCredentials.fromUserAndPassword(
                                (String) ClickHouseDefaults.USER.getEffectiveDefaultValue(),
                                (String) ClickHouseDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:ch:grpc://localhost", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:clickhouse:grpc://localhost:9100/default"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.GRPC)
                        .database((String) ClickHouseDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ClickHouseCredentials.fromUserAndPassword(
                                (String) ClickHouseDefaults.USER.getEffectiveDefaultValue(),
                                (String) ClickHouseDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:ch:https://:letmein@[::1]:3218/db1?user=aaa", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:clickhouse:http://[::1]:3218/db1"));
        Assert.assertEquals(info.getServer(), ClickHouseNode.builder().host("[::1]").port(ClickHouseProtocol.HTTP, 3218)
                .database("db1").credentials(ClickHouseCredentials.fromUserAndPassword("aaa", "letmein")).build());
        Assert.assertEquals(info.getProperties().getProperty("user"), "aaa");
    }

    @Test(groups = "unit")
    public void testParse() throws URISyntaxException {
        ConnectionInfo info = ClickHouseJdbcUrlParser.parse("jdbc:ch://localhost", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:clickhouse:http://localhost:8123/default"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.HTTP)
                        .database((String) ClickHouseDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ClickHouseCredentials.fromUserAndPassword(
                                (String) ClickHouseDefaults.USER.getEffectiveDefaultValue(),
                                (String) ClickHouseDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:ch:grpc://localhost", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:clickhouse:grpc://localhost:9100/default"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.GRPC)
                        .database((String) ClickHouseDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ClickHouseCredentials.fromUserAndPassword(
                                (String) ClickHouseDefaults.USER.getEffectiveDefaultValue(),
                                (String) ClickHouseDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:ch:https://:letmein@127.0.0.1:3218/db1", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:clickhouse:http://127.0.0.1:3218/db1"));
        Assert.assertEquals(info.getServer(), ClickHouseNode.builder().host("127.0.0.1")
                .port(ClickHouseProtocol.HTTP, 3218).database("db1")
                .credentials(ClickHouseCredentials
                        .fromUserAndPassword((String) ClickHouseDefaults.USER.getEffectiveDefaultValue(), "letmein"))
                .build());
    }

    @Test(groups = "unit")
    public void testParseWithProperties() throws URISyntaxException {
        ConnectionInfo info = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://localhost/", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:clickhouse:http://localhost:8123/default"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.HTTP)
                        .database((String) ClickHouseDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ClickHouseCredentials.fromUserAndPassword(
                                (String) ClickHouseDefaults.USER.getEffectiveDefaultValue(),
                                (String) ClickHouseDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        info = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://localhost:4321/ndb", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:clickhouse:http://localhost:4321/ndb"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.HTTP, 4321).database("ndb")
                        .credentials(ClickHouseCredentials.fromUserAndPassword(
                                (String) ClickHouseDefaults.USER.getEffectiveDefaultValue(),
                                (String) ClickHouseDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        Properties props = new Properties();
        props.setProperty("database", "db1");
        info = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://me@localhost:1234/mydb?password=123", props);
        Assert.assertEquals(info.getUri(), new URI("jdbc:clickhouse:http://localhost:1234/db1"));
        Assert.assertEquals(info.getServer(),
                ClickHouseNode.builder().host("localhost").port(ClickHouseProtocol.HTTP, 1234).database("db1")
                        .credentials(ClickHouseCredentials.fromUserAndPassword("me", "123")).build());
        Assert.assertEquals(info.getProperties().getProperty("database"), "db1");
    }

    @Test(groups = "unit")
    public void testParseCredentials() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "default1");
        props.setProperty("password", "password1");
        ClickHouseNode server = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://user:a:passwd@foo.ch/test", props)
                .getServer();
        Assert.assertEquals(server.getCredentials().get().getUserName(), "default1");
        Assert.assertEquals(server.getCredentials().get().getPassword(), "password1");

        server = ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://let%40me%3Ain:let%40me%3Ain@foo.ch", null)
                .getServer();
        Assert.assertEquals(server.getCredentials().get().getUserName(), "let@me:in");
        Assert.assertEquals(server.getCredentials().get().getPassword(), "let@me:in");
    }
}
