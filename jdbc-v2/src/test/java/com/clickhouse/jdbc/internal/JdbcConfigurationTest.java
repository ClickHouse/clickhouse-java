package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.ClientConfigProperties;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.DriverPropertyInfo;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class JdbcConfigurationTest {
    @Test(dataProvider = "testConnectionUrlDataProvider")
    public void testConnectionUrl(String jdbcUrl, String connectionUrl) {
        JdbcConfiguration configuration = new JdbcConfiguration(jdbcUrl, new Properties());
        assertEquals(configuration.getConnectionUrl(), connectionUrl);
    }

    @DataProvider(name = "testConnectionUrlDataProvider")
    public static Object[][] testConnectionUrlDataProvider() {
        return new Object[][] {
                {"jdbc:clickhouse://localhost:8123/", "https://localhost:8123/"},
                {"jdbc:clickhouse://localhost:8443/clickhouse?param1=value1&param2=value2", "https://localhost:8443/clickhouse?param1=value1&param2=value2"},
                {"jdbc:clickhouse:http://localhost:8123/clickhouse?param1=value1&param2=value2", "http://localhost:8123/clickhouse?param1=value1&param2=value2"}
        };
    }

    @Test
    public void testConfigurationProperties() throws Exception {

        Properties properties = new Properties();
        properties.setProperty(ClientConfigProperties.DATABASE.getKey(), "default2");
        properties.setProperty(DriverProperties.DEFAULT_QUERY_SETTINGS.getKey(),
                ClientConfigProperties.commaSeparated(Arrays.asList("http_headers=header1=3,header2=4")));
        String url = "jdbc:clickhouse://localhost:8123/clickhouse?client_name=test_application&database=default1";
        JdbcConfiguration configuration = new JdbcConfiguration(url, properties);
        Assert.assertEquals(configuration.getConnectionUrl(), "https://localhost:8123/clickhouse?client_name=test_application&database=default1");
        Map<String, DriverPropertyInfo> infos = configuration.getDriverPropertyInfo().stream().collect(Collectors.toMap(d -> d.name, d -> d));

        DriverPropertyInfo p = infos.get(ClientConfigProperties.DATABASE.getKey());
        Assert.assertEquals(p.value, "default2");

    }
}
