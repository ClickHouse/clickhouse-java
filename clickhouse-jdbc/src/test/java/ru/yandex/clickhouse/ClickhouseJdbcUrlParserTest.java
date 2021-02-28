package ru.yandex.clickhouse;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class ClickhouseJdbcUrlParserTest {

    @Test
    public void testParseDashes() throws Exception {
        Properties props = new Properties();
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
            "jdbc:clickhouse://foo.yandex:1337/db-name-with-dash", new Properties());
        Assert.assertEquals(chProps.getDatabase(), "db-name-with-dash");
    }

    @Test
    public void testParseTrailingSlash() throws Exception {
        Properties props = new Properties();
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
            "jdbc:clickhouse://foo.yandex:1337/", new Properties());
        Assert.assertEquals(chProps.getDatabase(), "default");
    }

    @Test
    public void testParseDbInPathAndProps() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setDatabase("database-name");
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
            "jdbc:clickhouse://foo.yandex:1337/database-name", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "database-name");
        Assert.assertEquals(chProps.getPath(), "/");
    }

    @Test
    public void testParseDbInPathAndProps2() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setDatabase("database-name");
        props.setUsePathAsDb(false);
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
            "jdbc:clickhouse://foo.yandex:1337/database-name", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "database-name");
        Assert.assertEquals(chProps.getPath(), "/database-name");
    }

    @Test
    public void testParsePathDefaultDb() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setPath("/path");
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
            "jdbc:clickhouse://foo.yandex:1337/", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "default");
        Assert.assertEquals(chProps.getPath(), "/path");
    }

    @Test
    public void testParsePathDefaultDb2() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setPath("/path");
        props.setUsePathAsDb(false);
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
            "jdbc:clickhouse://foo.yandex:1337", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "default");
        Assert.assertEquals(chProps.getPath(), "/"); //uri takes priority
    }

    @Test
    public void testParsePathAndDb() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
            "jdbc:clickhouse://foo.yandex:1337/db?database=dbname", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "db");
        Assert.assertEquals(chProps.getPath(), "/");
    }

    @Test
    public void testParsePathAndDb2() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUsePathAsDb(false);
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
            "jdbc:clickhouse://foo.yandex:1337/db?database=dbname", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "dbname");
        Assert.assertEquals(chProps.getPath(), "/db");
    }

}
