package ru.yandex.clickhouse.settings;

import org.testng.Assert;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.net.URI;
import java.util.Map;
import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

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
        Long maxInsertBlockSize = 142L;
        Boolean insertDeduplicate = true;
        Boolean insertDistributedSync = true;

        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setConnectionTimeout( expectedConnectionTimeout );
        properties.setMaxParallelReplicas( maxParallelReplicas );
        properties.setCompress( isCompress );
        properties.setMaxInsertBlockSize(maxInsertBlockSize);
        properties.setInsertDeduplicate(insertDeduplicate);
        properties.setInsertDistributedSync(insertDistributedSync);

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
        Assert.assertEquals(
            clickHouseDataSource.getProperties().getMaxInsertBlockSize(),
            maxInsertBlockSize
        );
        Assert.assertEquals(
            clickHouseDataSource.getProperties().getInsertDeduplicate(),
            insertDeduplicate
        );
        Assert.assertEquals(
            clickHouseDataSource.getProperties().getInsertDistributedSync(),
            insertDistributedSync
        );
    }

    @Test
    public void additionalParametersTest_clickhouse_datasource() {
        ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:1234/ppc?compress=1&decompress=1&user=root");

        assertTrue(clickHouseDataSource.getProperties().isCompress());
        assertTrue(clickHouseDataSource.getProperties().isDecompress());
        assertEquals("root", clickHouseDataSource.getProperties().getUser());
    }

    @Test
    public void additionalParametersTest_balanced_clickhouse_datasource() {
        BalancedClickhouseDataSource clickHouseDataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://localhost:1234,another.host.com:4321/ppc?compress=1&decompress=1&user=root");

        assertTrue(clickHouseDataSource.getProperties().isCompress());
        assertTrue(clickHouseDataSource.getProperties().isDecompress());
        assertEquals("root", clickHouseDataSource.getProperties().getUser());
    }

    @Test
    public void booleanParamCanBeParsedAsZeroAndOne() throws Exception {
        Assert.assertTrue(new ClickHouseProperties().isCompress());
        Assert.assertFalse(new ClickHouseProperties(new Properties(){{setProperty("compress", "0");}}).isCompress());
        Assert.assertTrue(new ClickHouseProperties(new Properties(){{setProperty("compress", "1");}}).isCompress());
    }

    @Test
    public void clickHouseQueryParamContainsMaxMemoryUsage() throws Exception {
        final ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        clickHouseProperties.setMaxMemoryUsage(43L);
        Assert.assertEquals(clickHouseProperties.asProperties().getProperty("max_memory_usage"), "43");
    }

    @Test
    public void maxMemoryUsageParamShouldBeParsed() throws Exception {
        final Properties driverProperties = new Properties();
        driverProperties.setProperty("max_memory_usage", "42");

        ClickHouseDataSource ds = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123/test", driverProperties);
        Assert.assertEquals(ds.getProperties().getMaxMemoryUsage(), Long.valueOf(42L), "max_memory_usage is missing");
    }

    @Test
    public void buildQueryParamsTest() {
        ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        clickHouseProperties.setInsertQuorumTimeout(1000L);
        clickHouseProperties.setInsertQuorum(3L);
        clickHouseProperties.setSelectSequentialConsistency(1L);
        clickHouseProperties.setMaxInsertBlockSize(42L);
        clickHouseProperties.setInsertDeduplicate(true);
        clickHouseProperties.setInsertDistributedSync(true);

        Map<ClickHouseQueryParam, String> clickHouseQueryParams = clickHouseProperties.buildQueryParams(true);
        Assert.assertEquals(clickHouseQueryParams.get(ClickHouseQueryParam.INSERT_QUORUM), "3");
        Assert.assertEquals(clickHouseQueryParams.get(ClickHouseQueryParam.INSERT_QUORUM_TIMEOUT), "1000");
        Assert.assertEquals(clickHouseQueryParams.get(ClickHouseQueryParam.SELECT_SEQUENTIAL_CONSISTENCY), "1");
        Assert.assertEquals(clickHouseQueryParams.get(ClickHouseQueryParam.MAX_INSERT_BLOCK_SIZE), "42");
        Assert.assertEquals(clickHouseQueryParams.get(ClickHouseQueryParam.INSERT_DEDUPLICATE), "1");
        Assert.assertEquals(clickHouseQueryParams.get(ClickHouseQueryParam.INSERT_DISTRIBUTED_SYNC), "1");
    }
}
