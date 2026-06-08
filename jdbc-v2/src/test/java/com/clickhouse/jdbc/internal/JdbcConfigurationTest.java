package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.DriverProperties;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class JdbcConfigurationTest {

    private static final  JdbcConfigurationTestData[] VALID_URLs = new JdbcConfigurationTestData[] {
        new JdbcConfigurationTestData("jdbc:ch://localhost"),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost"),
        new JdbcConfigurationTestData("jdbc:clickhouse:http://localhost"),
        new JdbcConfigurationTestData("jdbc:clickhouse:https://localhost")
            .withExpectedConnectionURL("https://localhost:8443"),
        new JdbcConfigurationTestData("jdbc:clickhouse:https://localhost:8123")
            .withExpectedConnectionURL("https://localhost:8123"),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost")
            .withAdditionalConnectionParameters(
                Map.of(JdbcConfiguration.USE_SSL_PROP, "true"))
            .withExpectedConnectionURL("https://localhost:8443"), // ssl should not be passed to client
        new JdbcConfigurationTestData("jdbc:clickhouse://[::1]")
            .withExpectedConnectionURL("http://[::1]:8123"),
        new JdbcConfigurationTestData("jdbc:clickhouse://[::1]:8123")
            .withExpectedConnectionURL("http://[::1]:8123"),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost:8443")
            .withExpectedConnectionURL("http://localhost:8443"),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost/database")
            .withAdditionalExpectedClientProperties(
                Map.of("database", "database")),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost:42/database")
            .withExpectedConnectionURL("http://localhost:42")
            .withAdditionalExpectedClientProperties(
                Map.of("database", "database")),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost/data-base")
            .withAdditionalExpectedClientProperties(
                Map.of("database", "data-base")),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost/data%20base")
            .withAdditionalExpectedClientProperties(
                Map.of("database", "data base")),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost/data%2Fbase")
            .withAdditionalExpectedClientProperties(
                Map.of("database", "data/base")),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost/☺")
            .withAdditionalExpectedClientProperties(
                Map.of("database", "☺")),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost/db?custom_key1=val1&custom_key2=val2")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "database", "db",
                    "custom_key1", "val1",
                    "custom_key2", "val2"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost/db?custom_key1=val%201")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "database", "db",
                    "custom_key1", "val 1"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost/?custom_key1=val1")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "custom_key1", "val1"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost?custom_key1=val1")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "custom_key1", "val1"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost:8123?custom_key1=val1")
            .withExpectedConnectionURL("http://localhost:8123")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "custom_key1", "val1"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost:8123/?custom_key1=val1")
            .withExpectedConnectionURL("http://localhost:8123")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "custom_key1", "val1"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost?custom_key1=☺")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "custom_key1", "☺"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost?custom_key1=val1,val2")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "custom_key1", "val1,val2"
                )),
        new JdbcConfigurationTestData(
            "jdbc:clickhouse://localhost:8443/default?http_header_roles=%22role%201,3,4%22,%27val2%27,val3&ssl=false")
                .withExpectedConnectionURL("http://localhost:8443")
                .withAdditionalExpectedClientProperties(
                    Map.of(
                        "database", "default",
                        "http_header_roles", "\"role 1,3,4\",'val2',val3"
                    ))
    };

    @SuppressWarnings("deprecation")
    @Test(dataProvider = "validURLTestData")
    public void testParseURLValid(String jdbcURL, Properties properties,
        String connectionURL, Map<String, String> expectedClientProps)
            throws Exception
    {
        JdbcConfiguration configuration = new JdbcConfiguration(jdbcURL, properties);
        assertEquals(configuration.getConnectionUrl(), connectionURL, "URL: " + jdbcURL);
        assertEquals(configuration.clientProperties, expectedClientProps, "URL: " + jdbcURL);
        Client.Builder bob = new Client.Builder();
        configuration.applyClientProperties(bob);
        Client client = bob.build();
        assertEquals(client.getEndpoints().size(), 1);

        String actualUrl = client.getEndpoints().iterator().next();
        if (actualUrl != null && Math.abs(actualUrl.length() - connectionURL.length()) == 1) {
            assertTrue(actualUrl.contains(connectionURL));
        } else {
            assertEquals(actualUrl, connectionURL);
        }
    }

    @Test(dataProvider = "invalidURLs")
    public void testParseURLInvalid(String jdbcURL) {
        assertThrows(
            SQLException.class,
            () -> new JdbcConfiguration(jdbcURL, new Properties()));

    }

    @Test(dataProvider = "validURLs")
    public void testAcceptsURLValid(String url) throws Exception {
        Assert.assertTrue(JdbcConfiguration.acceptsURL(url), "URL: " + url);
    }

    @Test
    public void testConfigurationProperties() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ClientConfigProperties.DATABASE.getKey(), "default2");
        properties.setProperty(DriverProperties.DEFAULT_QUERY_SETTINGS.getKey(),
            ClientConfigProperties.commaSeparated(Arrays.asList("http_headers=header1=3,header2=4")));
        String url = "jdbc:clickhouse://localhost:8123/clickhouse?client_name=test_application&database=default1";
        JdbcConfiguration configuration = new JdbcConfiguration(url, properties);
        assertEquals(configuration.getConnectionUrl(), "http://localhost:8123");
        DriverPropertyInfo p = configuration.getDriverPropertyInfo().stream()
            .filter(dpi -> ClientConfigProperties.DATABASE.getKey().equals(dpi.name))
            .findAny()
            .orElseThrow();
        assertEquals(p.value, "default1");
    }

    @DataProvider(name = "typeMappingsPropertyKey")
    public Object[][] typeMappingsPropertyKey() {
        return new Object[][] {
            { DriverProperties.JDBC_TYPE_MAPPINGS.getKey() }, // "jdbc_type_mappings"
            { DriverProperties.TYPE_MAPPINGS.getKey() },      // deprecated alias "typeMappings"
        };
    }

    /**
     * Verifies that both {@code jdbc_type_mappings} and the deprecated {@code typeMappings}
     * are accepted via {@link Properties} and URL parameters in the comma-separated {@code key=value} format
     * (e.g. {@code UInt64=java.lang.String}), are recognized as driver properties (kept in
     * driver properties and NOT leaked into client properties), and survive round-tripping
     * untouched so downstream consumers can parse them with
     * {@link ClientConfigProperties#toKeyValuePairs(String)}.
     */
    @SuppressWarnings("deprecation")
    @Test(dataProvider = "typeMappingsPropertyKey")
    public void testTypeMappingsPropertyAcceptsCommaSeparatedKeyValuePairs(String propertyKey)
        throws SQLException
    {
        String[] stringMappedTypes = {
            "UInt64", "UInt128", "Int128", "UInt256", "Int256",
            "Float32", "Float64", "BFloat16",
            "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256",
            "DateTime64"
        };

        Map<String, String> expected = new LinkedHashMap<>();
        for (String type : stringMappedTypes) {
            expected.put(type, "String");
        }
        expected.put("Enum8", "Byte");
        expected.put("UInt256", "String");

        StringBuilder mappingValueBuilder = new StringBuilder();
        for (Map.Entry<String, String> e : expected.entrySet()) {
            if (mappingValueBuilder.length() > 0) {
                mappingValueBuilder.append(',');
            }
            mappingValueBuilder.append(e.getKey()).append('=').append(e.getValue());
        }
        String mappingValue = mappingValueBuilder.toString();

        // 1. Test via Properties object
        Properties properties = new Properties();
        properties.setProperty(propertyKey, mappingValue);
        JdbcConfiguration configurationProp = new JdbcConfiguration("jdbc:clickhouse://localhost:8123", properties);
        verifyConfiguration(configurationProp, propertyKey, mappingValue, expected);

        // 2. Test via URL encoded value
        String encodedMappingValue = java.net.URLEncoder.encode(mappingValue, java.nio.charset.StandardCharsets.UTF_8);
        JdbcConfiguration configurationUrl = new JdbcConfiguration("jdbc:clickhouse://localhost:8123?" + propertyKey + "=" + encodedMappingValue, new Properties());
        verifyConfiguration(configurationUrl, propertyKey, mappingValue, expected);

        // 3. Test with raw (unencoded) value in URL which should also work due to how split("=", 2) works
        JdbcConfiguration configurationRawUrl = new JdbcConfiguration("jdbc:clickhouse://localhost:8123?" + propertyKey + "=" + mappingValue, new Properties());
        verifyConfiguration(configurationRawUrl, propertyKey, mappingValue, expected);
    }

    private void verifyConfiguration(JdbcConfiguration configuration, String propertyKey, String mappingValue, Map<String, String> expected) {
        assertEquals(
            configuration.getDriverProperty(propertyKey, null),
            mappingValue,
            "Driver property '" + propertyKey + "' should be preserved verbatim");

        assertFalse(
            configuration.getClientProperties().containsKey(propertyKey),
            "Driver property '" + propertyKey + "' must not leak into client properties");

        Map<String, String> parsed = ClientConfigProperties.toKeyValuePairs(
            configuration.getDriverProperty(propertyKey, null));
        assertEquals(parsed, expected, "Value should be parseable as CSV key=value pairs");

        Map<String, Class<?>> expectedClasses = new HashMap<>();
        expectedClasses.put("String", String.class);
        expectedClasses.put("Byte", Byte.class);
        expectedClasses.put("ClickHouseDataType", ClickHouseDataType.class);

        Map<String, Class<?>> typeMap = configuration.getTypeMap();
        for (String key : typeMap.keySet()) {
            Class<?> expectedClass = expectedClasses.get(expected.get(key));
            assertEquals(typeMap.get(key), expectedClass, "Type mapping for '" + key + "'");
        }
    }

    @Test
    public void testTypeMappingsThrowsIfBothPropertiesProvided() {
        Properties properties = new Properties();
        properties.setProperty(DriverProperties.JDBC_TYPE_MAPPINGS.getKey(), "UInt64=java.lang.String");
        properties.setProperty(DriverProperties.TYPE_MAPPINGS.getKey(), "UInt64=java.lang.String");

        assertThrows(SQLException.class, () -> 
            new JdbcConfiguration("jdbc:clickhouse://localhost:8123", properties)
        );
    }

    @Test
    public void testTypeMappingsThrowsIfClassNotFound() {
        Properties properties = new Properties();
        properties.setProperty(DriverProperties.JDBC_TYPE_MAPPINGS.getKey(), "UInt64=java.lang.UnknownClassXXX");

        assertThrows(SQLException.class, () -> 
            new JdbcConfiguration("jdbc:clickhouse://localhost:8123", properties)
        );
    }

    @DataProvider(name = "validURLTestData")
    public Object[][] createValidConnectionURLTestData() {
        return Arrays.stream(VALID_URLs)
            .map(d -> new Object[] {
                d.url,
                d.connectionParameters,
                d.expectedConnectionURL,
                d.expectedClientProperties
            })
            .toArray(Object[][]::new);
    }

    @DataProvider(name = "validURLs")
    public Object[][] createValidConnectionURLs() {
        return Arrays.stream(VALID_URLs)
            .map(d -> new Object[] {
                d.url
            })
            .toArray(Object[][]::new);
    }

    @DataProvider(name = "invalidURLs")
    public Object[][] createInvalidConnectionURLs() {
        return new String[][] {
            { null },
            { "" },
            { "    " },
            { " \r\n \t " },
            { "jdbc:error://foo.bar" },
            { "https://clickhouse.com" },
            { "jdbc:clickhouse: //foo.bar" },
            { "jdbc:ch" },
            { "jdbc:clickhouse://" },
            { "jdbc:clickhouse://  /" },
            { "jdbc:clickhouse:////foo.bar" },
            { "jdbc:clickhouse://local host" },
            { "jdbc:clickhouse://foo.bar/data base" },
            { "jdbc:clickhouse://foo.bar? = " },
            { "jdbc:clickhouse://foo.bar?x= " },
            { "jdbc:clickhouse://foo.bar? =x" },
            { "jdbc:clickhouse://foo.bar?%20=" },
            { "jdbc:clickhouse://foo.bar?%20=%20" },
            { "jdbc:clickhouse://foo.bar?x=%20%20" },
            { "jdbc:clickhouse://localhost/?key%201=val1" },
            { "jdbc:clickhouse://localhost/db?%20key1%20=%20val%20" },
            { "jdbc:clickhouse://foo.bar?x&y=z" },
            { "jdbc:clickhouse://foo.bar?x==&y=z" },
            { "jdbc:clickhouse://localhost?☺=value1" },
            // multiple endpoints are invalid
            { "jdbc:clickhouse://foo,bar" },
            { "jdbc:clickhouse://foo,bar.com:8123" },
        };
    }

    private static final class JdbcConfigurationTestData {

        private static final Map<String, String> DEFAULT_CONNECTION_PARAMS =
            Map.of( "user", "default", "password", "");

        private static final Map<String, String> DEFAULT_EXPECTED_CLIENT_PROPERTIES =
            Map.of( "user", "default", "password", "");

        private static final String DEFAULT_EXPECTED_CONNECTION_URL =
            "http://localhost:8123";

        private final String url;
        private final Properties connectionParameters;
        private String expectedConnectionURL;
        private final Map<String, String> expectedClientProperties;

        JdbcConfigurationTestData(String url) {
            this.url = Objects.requireNonNull(url);
            this.connectionParameters = new Properties();
            this.connectionParameters.putAll(DEFAULT_CONNECTION_PARAMS);
            this.expectedConnectionURL = DEFAULT_EXPECTED_CONNECTION_URL;
            this.expectedClientProperties = new HashMap<>(
                DEFAULT_EXPECTED_CLIENT_PROPERTIES); // modifiable
        }

        JdbcConfigurationTestData withAdditionalConnectionParameters(
            Map<String, String> additionalConnectionParameters)
        {
            this.connectionParameters.putAll(additionalConnectionParameters);
            return this;
        }

        JdbcConfigurationTestData withExpectedConnectionURL(
            String expectedConnectionURL)
        {
            this.expectedConnectionURL = Objects.requireNonNull(expectedConnectionURL);
            return this;
        }

        JdbcConfigurationTestData withAdditionalExpectedClientProperties(
            Map<String, String> additionalExpectedClientProperties)
        {
            this.expectedClientProperties.putAll(
                additionalExpectedClientProperties);
            return this;
        }

    }

}
