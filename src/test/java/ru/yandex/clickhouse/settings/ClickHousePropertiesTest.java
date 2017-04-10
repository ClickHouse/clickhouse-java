package ru.yandex.clickhouse.settings;

import org.testng.Assert;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.net.URI;
import java.util.Properties;

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

    @Test
    public void constructorShouldNotIgnoreClickHouseProperties() {
        int expectedConnectionTimeout = 1000;
        boolean isCompress = false;
        Integer maxParallelReplicas = 3;

        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setConnectionTimeout( expectedConnectionTimeout );
        properties.setMaxParallelReplicas( maxParallelReplicas );
        properties.setCompress( isCompress );

        ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://localhost:8123/test",
                properties
        );
        Assert.assertEquals(
                clickHouseDataSource.getProperties().getConnectionTimeout(),
                expectedConnectionTimeout
        );
        Assert.assertEquals(
                clickHouseDataSource.getProperties().isCompress(),
                isCompress
        );
        Assert.assertEquals(
                clickHouseDataSource.getProperties().getMaxParallelReplicas(),
                maxParallelReplicas
        );
        Assert.assertEquals(
                clickHouseDataSource.getProperties().getTotalsMode(),
                ClickHouseQueryParam.TOTALS_MODE.getDefaultValue()
        );
    }

    @Test
    public void booleanParamCanBeParsedAsZeroAndOne() throws Exception {
        Assert.assertTrue(new ClickHouseProperties().isCompress());
        Assert.assertFalse(new ClickHouseProperties(new Properties(){{setProperty("compress", "0");}}).isCompress());
        Assert.assertTrue(new ClickHouseProperties(new Properties(){{setProperty("compress", "1");}}).isCompress());
    }
}
