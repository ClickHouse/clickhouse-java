package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.Driver;
import com.clickhouse.jdbc.DriverProperties;
import com.google.common.collect.ImmutableMap;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JdbcConfiguration {

    private static final String PREFIX_CLICKHOUSE = "jdbc:clickhouse:";
    private static final String PREFIX_CLICKHOUSE_SHORT = "jdbc:ch:";
    static final String USE_SSL_PROP = "ssl";

    private static final String PARSE_URL_CONN_URL_PROP = "connection_url";
    private static final Pattern PATTERN_HTTP_TOKEN = Pattern.compile(
        "[A-Za-z0-9!#$%&'*+\\.\\^_`\\|~-]+");

    private final boolean disableFrameworkDetection;

    final Map<String, String> clientProperties;
    public Map<String, String> getClientProperties() {
        return ImmutableMap.copyOf(clientProperties);
    }

    private final Map<String, String> driverProperties;

    private final String connectionUrl;

    public boolean isDisableFrameworkDetection() {
        return disableFrameworkDetection;
    }

    private boolean isIgnoreUnsupportedRequests;

    public boolean isIgnoreUnsupportedRequests() {
        return isIgnoreUnsupportedRequests;
    }

    /**
     * Parses URL to get property and target host.
     * Properties that are passed in the {@code info} parameter will override that are set in the {@code url}.
     * @param url - JDBC url
     * @param info - Driver and Client properties.
     */
    public JdbcConfiguration(String url, Properties info) throws SQLException {
        this.disableFrameworkDetection = info != null && Boolean.parseBoolean(info.getProperty("disable_frameworks_detection", "false"));
        this.clientProperties = new HashMap<>();
        this.driverProperties = new HashMap<>();

        Map<String, String> urlProperties = parseUrl(url);
        String tmpConnectionUrl = urlProperties.remove(PARSE_URL_CONN_URL_PROP);
        initProperties(urlProperties, info);

        // after initializing all properties - set final connection URL
        boolean useSSLInfo = Boolean.parseBoolean(info.getProperty(DriverProperties.SECURE_CONNECTION.getKey(), "false"));
        boolean useSSLUrlProperties = Boolean.parseBoolean(urlProperties.getOrDefault(DriverProperties.SECURE_CONNECTION.getKey(), "false"));
        boolean useSSL = useSSLInfo || useSSLUrlProperties;
        String bearerToken = info.getProperty(ClientConfigProperties.BEARERTOKEN_AUTH.getKey(), null);
        if (bearerToken != null) {
            clientProperties.put(ClientConfigProperties.BEARERTOKEN_AUTH.getKey(), bearerToken);
        }

        this.connectionUrl = createConnectionURL(tmpConnectionUrl, useSSL);
        this.isIgnoreUnsupportedRequests = Boolean.parseBoolean(getDriverProperty(DriverProperties.IGNORE_UNSUPPORTED_VALUES.getKey(), "false"));
    }

    /**
     * This method (only) checks if this driver is probably responsible for the
     * connection as given in {@code url}, no further sanity checks are
     * performed.
     *
     * @param url
     *            the JDBC connection URL
     * @return {@link true} if ClickHouse JDBC driver is responsible for
     *         connection, {@code false} else
     * @throws SQLException
     *             if there is a technical error parsing the {@code url}
     */
    public static boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("URL is null");
        }
        if (!url.startsWith(PREFIX_CLICKHOUSE)
            && !url.startsWith(PREFIX_CLICKHOUSE_SHORT))
        {
            return false;
        }
        try {
            URI uri = new URI(url);
            // make sure uri is used
            return "jdbc".equals(uri.getScheme());
        } catch (URISyntaxException urise) {
            throw new SQLException(
                "Not a valid URL '" + url + "'. ", urise);
        }
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    /**
     * Returns normalized URL that can be passed as parameter to Client#addEndpoint().
     * Returned url has only schema and authority and doesn't have query parameters or path.
     * JDBC URL should have only a single path parameter to specify database name.
     * Note: Some BI tools do not let pass JDBC URL, so ssl is passed as property.
     * @param url - JDBC url
     * @param ssl - if SSL protocol should be used
     * @return URL without JDBC prefix
     */
    private static String createConnectionURL(String url, boolean ssl) throws SQLException {
        String adjustedURL = ssl && url.startsWith("http://")
            ? "https://" + url.substring(7)
            : url;
        try {
            URI tmp = URI.create(adjustedURL);
            String asciiString = tmp.toASCIIString();
            if (tmp.getPort() < 0) {
                String port = ssl || adjustedURL.startsWith("https")
                    ? ":" + ClickHouseHttpProto.DEFAULT_HTTPS_PORT
                    : ":" + ClickHouseHttpProto.DEFAULT_HTTP_PORT;
                asciiString += port;
            }
            return asciiString;
        } catch (IllegalArgumentException iae) {
            throw new SQLException("Failed to parse URL '" + url + "'", iae);
        }
    }

    private static String stripJDBCPrefix(String url) {
        if (url.startsWith(PREFIX_CLICKHOUSE)) {
            return url.substring(PREFIX_CLICKHOUSE.length());
        } else if (url.startsWith(PREFIX_CLICKHOUSE_SHORT)) {
            return url.substring(PREFIX_CLICKHOUSE_SHORT.length());
        } else {
            throw new IllegalArgumentException("Specified JDBC URL doesn't have any of prefixes: [ "
                + PREFIX_CLICKHOUSE + ", " + PREFIX_CLICKHOUSE_SHORT + " ]");
        }
    }

    private List<DriverPropertyInfo> listOfProperties;

    private Map<String, String> parseUrl(String url) throws SQLException {
        Map<String, String> properties = new HashMap<>();
        String myURL = null;
        try {
            myURL = stripJDBCPrefix(url);
        } catch (Exception e) {
            throw new SQLException(
                "Error determining JDBC prefix from URL '" + url + "'", e);
        }
        if (myURL.startsWith("//")) {
            myURL = "http://" + myURL.substring(2);
        }
        URI uri = null;
        try {
            uri = new URI(myURL);
        } catch (URISyntaxException urise) {
            throw new SQLException(
                "Invalid JDBC URL '" + url + "'", urise);
        }
        if (uri.getAuthority() == null) {
            throw new SQLException(
                "Invalid authority part JDBC URL '" + url + "'");
        }
        if (uri.getAuthority().contains(",")) {
            throw new SQLException("Multiple endpoints not supported");
        }
        properties.put(PARSE_URL_CONN_URL_PROP, uri.getScheme() + "://"
            + uri.getRawAuthority()); // will be parsed again later

        if (uri.getPath() != null
            && !uri.getPath().trim().isEmpty()
            && !"/".equals(uri.getPath()))
        {
            properties.put(
                ClientConfigProperties.DATABASE.getKey(),
                uri.getPath().substring(1));
        }
        if (uri.getQuery() != null && !uri.getQuery().trim().isEmpty()) {
            for (String pair : uri.getRawQuery().split("&")) {
                try {
                    String[] p = pair.split("=", 2);
                    if (p.length != 2 || p[0] == null || p[1] == null) {
                        throw new SQLException("Invalid query parameter '" + pair + "'");
                    }
                    String key = URLDecoder.decode(p[0], StandardCharsets.UTF_8.name());
                    if (key == null || key.trim().isEmpty() || !PATTERN_HTTP_TOKEN.matcher(key).matches()) {
                        throw new SQLException("Invalid query parameter key in pair'" + pair + "'");
                    }
                    String value = URLDecoder.decode(p[1], StandardCharsets.UTF_8.name());
                    if (value == null || value.trim().isEmpty() || "=".equals(value)) {
                        throw new SQLException("Invalid query parameter value in pair '" + pair + "'");
                    }
                    properties.put(key.trim(), value);
                } catch (UnsupportedEncodingException e) {
                    throw new SQLException("Internal error'", e);
                }
            }
        }
        return properties;
    }

    private void initProperties(Map<String, String> urlProperties, Properties providedProperties) {

        // Copy provided properties
        Map<String, String> props = new HashMap<>();
        for (Map.Entry<Object, Object> entry : providedProperties.entrySet()) {
            if (entry.getKey() instanceof String && entry.getValue() instanceof String) {
                props.put((String) entry.getKey(), (String) entry.getValue());
            } else {
                throw new IllegalArgumentException("Property key and value should be a string");
            }
        }

        props.putAll(urlProperties);

        // Process all properties
        Map<String, DriverPropertyInfo> propertyInfos = new HashMap<>();

        // create initial list of properties that will be passed to a client
        for (Map.Entry<String, String> prop : props.entrySet()) {
            DriverPropertyInfo propertyInfo = new DriverPropertyInfo(prop.getKey(), prop.getValue());
            propertyInfo.description = "(User Defined)";
            propertyInfos.put(prop.getKey(), propertyInfo);
            clientProperties.put(prop.getKey(), prop.getValue());
        }

        // Fill list of client properties information, add not specified properties (doesn't affect client properties)
        for (ClientConfigProperties clientProp : ClientConfigProperties.values()) {
            DriverPropertyInfo propertyInfo = propertyInfos.get(clientProp.getKey());
            if (propertyInfo == null) {
                propertyInfo = new DriverPropertyInfo(clientProp.getKey(), clientProp.getDefaultValue());
                // TODO: read description from resource file
                propertyInfos.put(clientProp.getKey(), propertyInfo);
            }
        }

        // Fill list of driver properties information, add not specified properties,
        // copy know driver properties from client properties
        for (DriverProperties driverProp : DriverProperties.values()) {
            DriverPropertyInfo propertyInfo = propertyInfos.get(driverProp.getKey());
            if (propertyInfo == null) {
                propertyInfo = new DriverPropertyInfo(driverProp.getKey(), driverProp.getDefaultValue());
                propertyInfos.put(driverProp.getKey(), propertyInfo);
            }

            String value = clientProperties.get(driverProp.getKey());
            if (value != null) {
                driverProperties.put(driverProp.getKey(), value);
            }
        }

        listOfProperties = propertyInfos.values().stream().sorted(Comparator.comparing(o -> o.name)).collect(Collectors.toList());
    }

    /**
     * Returns a list of driver property information.
     * @return a list of driver property information for the driver
     */
    public List<DriverPropertyInfo> getDriverPropertyInfo() {
        return listOfProperties;
    }

    public String getDriverProperty(String key, String defaultValue) {
        return driverProperties.getOrDefault(key, defaultValue);
    }

    public Boolean isSet(DriverProperties driverProp) {
        String v = driverProperties.getOrDefault(driverProp.getKey(), driverProp.getDefaultValue());
        return Boolean.parseBoolean(v);
    }

    public Client.Builder applyClientProperties(Client.Builder builder) {
        builder.addEndpoint(connectionUrl)
                .setOptions(clientProperties)
                .typeHintMapping(defaultTypeHintMapping());
        return builder;
    }

    public void updateUserClient(String clientName, Client client) {
        client.updateClientName((clientName == null || clientName.isEmpty() ? "" : clientName) + ' ' + getDefaultClientName());
    }

    public static String getDefaultClientName() {
        StringBuilder jdbcName = new StringBuilder();
        jdbcName.append(Driver.DRIVER_CLIENT_NAME)
                        .append(Driver.getLibraryVersion());

        return jdbcName.toString();
    }

    public boolean isBetaFeatureEnabled(DriverProperties prop) {
        return isFlagSet(prop);
    }

    public boolean isFlagSet(DriverProperties prop) {
        String value = driverProperties.getOrDefault(prop.getKey(), prop.getDefaultValue());
        return Boolean.parseBoolean(value);
    }

    private Map<ClickHouseDataType, Class<?>> defaultTypeHintMapping() {
        Map<ClickHouseDataType, Class<?>> mapping = new HashMap<>();
        mapping.put(ClickHouseDataType.Array, List.class);
        return mapping;
    }

    public boolean useMaxResultRows() {
        return isFlagSet(DriverProperties.USE_MAX_RESULT_ROWS);
    }
}
