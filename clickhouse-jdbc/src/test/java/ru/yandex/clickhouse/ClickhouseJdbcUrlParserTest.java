package ru.yandex.clickhouse;

import java.util.Properties;

import com.clickhouse.client.ClickHouseProtocol;

import org.eclipse.jetty.client.ProxyProtocolClientConnectionFactory.V2.Tag.Protocol;
import org.testng.Assert;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class ClickhouseJdbcUrlParserTest {

    @Test(groups = "unit")
    public void testParseDashes() throws Exception {
        Properties props = new Properties();
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser
                .parse("jdbc:clickhouse://foo.yandex:1337/db-name-with-dash", new Properties());
        Assert.assertEquals(chProps.getDatabase(), "db-name-with-dash");
    }

    @Test(groups = "unit")
    public void testParseTrailingSlash() throws Exception {
        Properties props = new Properties();
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse("jdbc:clickhouse://foo.yandex:1337/",
                new Properties());
        Assert.assertEquals(chProps.getDatabase(), "default");
    }

    @Test(groups = "unit")
    public void testParseDbInPathAndProps() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setDatabase("database-name");
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse("jdbc:clickhouse://foo.yandex:1337/database-name",
                props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "database-name");
        Assert.assertEquals(chProps.getPath(), "/");
    }

    @Test(groups = "unit")
    public void testParseDbInPathAndProps2() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setDatabase("database-name");
        props.setUsePathAsDb(false);
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse("jdbc:clickhouse://foo.yandex:1337/database-name",
                props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "database-name");
        Assert.assertEquals(chProps.getPath(), "/database-name");
    }

    @Test(groups = "unit")
    public void testParsePathDefaultDb() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setPath("/path");
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse("jdbc:clickhouse://foo.yandex:1337/",
                props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "default");
        Assert.assertEquals(chProps.getPath(), "/path");
    }

    @Test(groups = "unit")
    public void testParsePathDefaultDb2() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setPath("/path");
        props.setUsePathAsDb(false);
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse("jdbc:clickhouse://foo.yandex:1337",
                props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "default");
        Assert.assertEquals(chProps.getPath(), "/"); // uri takes priority
    }

    @Test(groups = "unit")
    public void testParsePathAndDb() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser
                .parse("jdbc:clickhouse://foo.yandex:1337/db?database=dbname", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "db");
        Assert.assertEquals(chProps.getPath(), "/");
    }

    @Test(groups = "unit")
    public void testParsePathAndDb2() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUsePathAsDb(false);
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser
                .parse("jdbc:clickhouse://foo.yandex:1337/db?database=dbname", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "dbname");
        Assert.assertEquals(chProps.getPath(), "/db");
    }

    @Test(groups = "unit")
    public void testParseCredentials() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUser("default1");
        props.setPassword("password1");
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse("jdbc:clickhouse://user:a:passwd@foo.ch/test",
                props.asProperties());
        Assert.assertEquals(chProps.getUser(), "user");
        Assert.assertEquals(chProps.getPassword(), "a:passwd");

        chProps = ClickhouseJdbcUrlParser.parse("jdbc:clickhouse://let%40me%3Ain:let%40me%3Ain@foo.ch",
                props.asProperties());
        Assert.assertEquals(chProps.getUser(), "let@me:in");
        Assert.assertEquals(chProps.getPassword(), "let@me:in");
    }

    @Test(groups = "unit")
    public void testParseProtocol() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse("jdbc:clickhouse://foo.ch/test",
                props.asProperties());
        Assert.assertEquals(chProps.getProtocol(), ClickHouseProtocol.HTTP);
        Assert.assertEquals(chProps.getPort(), ClickHouseProtocol.HTTP.getDefaultPort());

        chProps = ClickhouseJdbcUrlParser.parse("jdbc:clickhouse://foo.ch/test?protocol=grpc", props.asProperties());
        Assert.assertEquals(chProps.getProtocol(), ClickHouseProtocol.GRPC);
        Assert.assertEquals(chProps.getPort(), ClickHouseProtocol.GRPC.getDefaultPort());
    }
}
