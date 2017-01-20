package ru.yandex.clickhouse.settings;

import java.net.URI;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHousePropertiesTest {

    /**
     * Method {@link ru.yandex.clickhouse.ClickhouseJdbcUrlParser#parseUriQueryPart(URI, Properties)} returns instance
     * of {@link Properties} with defaults. These defaults may be missed if method
     * {@link java.util.Hashtable#get(Object)} is used for {@code Properties}.
     */
    @Test
    public void constructorShouldNotIgnoreDefaults() {
        Properties defaults = new Properties();
        String expectedUsername = "superuser";
        defaults.setProperty("user", expectedUsername);
        Properties propertiesWithDefaults = new Properties(defaults);

        ClickHouseProperties clickHouseProperties = new ClickHouseProperties(propertiesWithDefaults);
        Assert.assertEquals(clickHouseProperties.getUser(), expectedUsername);
    }

}
