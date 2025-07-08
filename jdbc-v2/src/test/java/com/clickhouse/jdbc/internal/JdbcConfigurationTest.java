package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class JdbcConfigurationTest {

    private static final JdbcConfigurationTestData[] VALID_URLs = new JdbcConfigurationTestData[] {
        new JdbcConfigurationTestData("jdbc:ch://localhost"),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost"),
        new JdbcConfigurationTestData("jdbc:clickhouse:http://localhost"),
        new JdbcConfigurationTestData("jdbc:clickhouse:https://localhost")
            .withExpectedConnectionURL("https://localhost:8443"),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost")
            .withAdditionalConnectionParameters(
                Map.of(JdbcConfiguration.USE_SSL_PROP, "true"))
            .withExpectedConnectionURL("https://localhost:8443")
            .withAdditionalExpectedClientProperties(
                Map.of("ssl", "true")),
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
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost/db?key1=val1&key2=val2")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "database", "db",
                    "key1", "val1",
                    "key2", "val2"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost/db?key1=val%201")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "database", "db",
                    "key1", "val 1"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost/?key1=val1")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "key1", "val1"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost?key1=val1")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "key1", "val1"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost:8123?key1=val1")
            .withExpectedConnectionURL("http://localhost:8123")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "key1", "val1"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost:8123/?key1=val1")
            .withExpectedConnectionURL("http://localhost:8123")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "key1", "val1"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost?key1=☺")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "key1", "☺"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://localhost?key1=val1,val2")
            .withAdditionalExpectedClientProperties(
                Map.of(
                    "key1", "val1,val2"
                )),
        new JdbcConfigurationTestData(
            "jdbc:clickhouse://localhost:8443/default?custom_header1=%22role%201,3,4%22,%27val2%27,val3&param1=value1")
                .withExpectedConnectionURL("http://localhost:8443")
                .withAdditionalExpectedClientProperties(
                    Map.of(
                        "database", "default",
                        "custom_header1", "\"role 1,3,4\",'val2',val3",
                        "param1", "value1"
                )),
        new JdbcConfigurationTestData("jdbc:clickhouse://foo:8443,bar:8123")
            .withExpectedConnectionURLs(
                Set.of(
                    "http://foo:8443",
                    "http://bar:8123")),
        new JdbcConfigurationTestData("jdbc:ch://foo.bar,baz.qux")
            .withExpectedConnectionURLs(
                Set.of(
                    "http://foo.bar:8123",
                    "http://baz.qux:8123")),
        new JdbcConfigurationTestData("jdbc:ch:https://foo.bar,baz.qux:4242")
            .withExpectedConnectionURLs(
                Set.of(
                    "https://foo.bar:8443",
                    "https://baz.qux:4242")),
        new JdbcConfigurationTestData("jdbc:ch:http://foo.bar,baz.qux:4242")
            .withAdditionalConnectionParameters(
                Map.of(JdbcConfiguration.USE_SSL_PROP, "true"))
            .withAdditionalExpectedClientProperties(
                Map.of(JdbcConfiguration.USE_SSL_PROP, "true"))
            .withExpectedConnectionURLs(
                Set.of(
                    "https://foo.bar:8443",
                    "https://baz.qux:4242")),
        new JdbcConfigurationTestData("jdbc:ch://127.0.0.1,baz.qux")
            .withExpectedConnectionURLs(
                Set.of(
                    "http://127.0.0.1:8123",
                    "http://baz.qux:8123")),
        new JdbcConfigurationTestData("jdbc:ch://foo,bar,baz")
            .withExpectedConnectionURLs(
                Set.of(
                    "http://foo:8123",
                    "http://bar:8123",
                    "http://baz:8123"))
    };

    @SuppressWarnings("deprecation")
    @Test(dataProvider = "validURLTestData")
    public void testParseURLValidAndApply(String jdbcURL, Properties properties,
        Set<String> connectionURLs, Map<String, String> expectedClientProps)
            throws Exception
    {
        JdbcConfiguration configuration = new JdbcConfiguration(jdbcURL, properties);
        assertEquals(configuration.getConnectionURLs(), connectionURLs);
        assertEquals(configuration.clientProperties, expectedClientProps);
        Client.Builder bob = new Client.Builder();
        configuration.applyClientProperties(bob);
        Client client = bob.build();
        assertEquals(
            client.getEndpoints(),
            connectionURLs);
        expectedClientProps.entrySet().forEach(
            e -> assertEquals(client.getConfiguration().get(e.getKey()), e.getValue()));
    }

    @Test(dataProvider = "invalidURLs")
    public void testParseURLInvalid(String jdbcURL) {
        assertThrows(
            SQLException.class,
            () -> new JdbcConfiguration(jdbcURL, new Properties()));

    }

    @Test(dataProvider = "validURLs")
    public void testAcceptsURLValid(String url) throws Exception {
        Assert.assertTrue(JdbcConfiguration.acceptsURL(url));
    }

    @Test
    public void testConfigurationProperties() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ClientConfigProperties.DATABASE.getKey(), "default2");
        properties.setProperty(DriverProperties.DEFAULT_QUERY_SETTINGS.getKey(),
            ClientConfigProperties.commaSeparated(Arrays.asList("http_headers=header1=3,header2=4")));
        String url = "jdbc:clickhouse://localhost:8123/clickhouse?client_name=test_application&database=default1";
        JdbcConfiguration configuration = new JdbcConfiguration(url, properties);
        assertEquals(configuration.getConnectionURLs().iterator().next(), "http://localhost:8123");
        DriverPropertyInfo p = configuration.getDriverPropertyInfo().stream()
            .filter(dpi -> ClientConfigProperties.DATABASE.getKey().equals(dpi.name))
            .findAny()
            .orElseThrow();
        assertEquals(p.value, "default1");
    }

    @DataProvider(name = "validURLTestData")
    public Object[][] createValidConnectionURLTestData() {
        return Arrays.stream(VALID_URLs)
            .map(d -> new Object[] {
                d.url,
                d.connectionParameters,
                d.expectedConnectionURLs,
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
            // would have to compare split() result length with number of commas
            // { "jdbc:clickhouse://foo,bar,/" },
            { "jdbc:clickhouse://foo,,bar" },
            { "jdbc:clickhouse://,foo,bar" },
            { "jdbc:clickhouse://,," },
            { "jdbc:clickhouse://,%20," },
            { "jdbc:clickhouse://foo,bar, /" },
            { "jdbc:clickhouse://foo,bar,%20/" },
            { "jdbc:clickhouse://foo,bar,:8123" },
            { "jdbc:clickhouse://foo,bar,:8123/db" },
            { "jdbc:clickhouse://[::1],[::2]" }
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
        private Set<String> expectedConnectionURLs;
        private final Map<String, String> expectedClientProperties;

        JdbcConfigurationTestData(String url) {
            this.url = Objects.requireNonNull(url);
            this.connectionParameters = new Properties();
            this.connectionParameters.putAll(DEFAULT_CONNECTION_PARAMS);
            this.expectedConnectionURLs = Collections.singleton(
                DEFAULT_EXPECTED_CONNECTION_URL);
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
            this.expectedConnectionURLs = Collections.singleton(expectedConnectionURL);
            return this;
        }

        JdbcConfigurationTestData withExpectedConnectionURLs(
            Collection<String> newExpectedConnectionURLs)
        {
            this.expectedConnectionURLs = new HashSet<>(newExpectedConnectionURLs);
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
