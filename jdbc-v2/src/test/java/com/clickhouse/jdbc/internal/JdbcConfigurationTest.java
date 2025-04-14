package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.ClientConfigProperties;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.DriverPropertyInfo;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class JdbcConfigurationTest {


    @Test(dataProvider = "testConnectionUrlDataProvider")
    public void testConnectionUrl(String jdbcUrl, String connectionUrl, Properties properties, Map<String, String> expectedClientProps) throws Exception {
        JdbcConfiguration configuration = new JdbcConfiguration(jdbcUrl, properties);
        assertEquals(configuration.getConnectionUrl(), connectionUrl);
        assertEquals(configuration.clientProperties, expectedClientProps);
    }

    @DataProvider(name = "testConnectionUrlDataProvider")
    public static Object[][] testConnectionUrlDataProvider() {
        Properties defaultProps = new Properties();
        defaultProps.setProperty(ClientConfigProperties.USER.getKey(), "default");
        defaultProps.setProperty(ClientConfigProperties.PASSWORD.getKey(), "");
        Properties useSSL = new Properties();
        useSSL.put(JdbcConfiguration.USE_SSL_PROP, "true");

        Map<String, String> defaultParams = Map.of( "user", "default", "password", "");
        Map<String, String> simpleParams = Map.of( "database", "clickhouse", "param1", "value1", "param2", "value2", "user", "default", "password", "");
        Map<String, String> useSSLParams = Map.of("ssl", "true");
        Map<String, String> withListParams = Map.of("database", "default", "param1", "value1", "custom_header1", "val1,val2,val3", "user", "default", "password", "");
        Map<String, String> withListParamsQuotes = Map.of("database", "default", "param1", "value1", "custom_header1", "\"role 1,3,4\",'val2',val3", "user", "default", "password", "");
        Map<String, String> useDatabaseSSLParams = Map.of("database", "clickhouse", "ssl", "true", "user", "default", "password", "");

        return new Object[][] {
                {"jdbc:clickhouse://localhost:8123/", "http://localhost:8123", defaultProps, defaultParams},
                {"jdbc:clickhouse://localhost:8443/clickhouse?param1=value1&param2=value2", "http://localhost:8443", defaultProps, simpleParams},
                {"jdbc:clickhouse:https://localhost:8123/clickhouse?param1=value1&param2=value2", "https://localhost:8123", defaultProps, simpleParams},
                {"jdbc:clickhouse://localhost:8443/", "https://localhost:8443", useSSL, useSSLParams},
                {"jdbc:clickhouse://localhost:8443/default?param1=value1&custom_header1=val1,val2,val3", "http://localhost:8443", defaultProps, withListParams},
                {"jdbc:clickhouse://localhost:8443/default?custom_header1=\"role 1,3,4\",'val2',val3&param1=value1", "http://localhost:8443", defaultProps, withListParamsQuotes},
                {"jdbc:clickhouse://localhost:8443/clickhouse?ssl=true", "https://localhost:8443", defaultProps, useDatabaseSSLParams},
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
        Assert.assertEquals(configuration.getConnectionUrl(), "http://localhost:8123");
        Map<String, DriverPropertyInfo> infos = configuration.getDriverPropertyInfo().stream().collect(Collectors.toMap(d -> d.name, d -> d));

        DriverPropertyInfo p = infos.get(ClientConfigProperties.DATABASE.getKey());
        Assert.assertEquals(p.value, "default1");
    }
}
