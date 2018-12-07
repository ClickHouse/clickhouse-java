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

}
