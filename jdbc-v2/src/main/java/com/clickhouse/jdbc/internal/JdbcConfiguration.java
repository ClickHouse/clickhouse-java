package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.jdbc.Driver;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JdbcConfiguration {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JdbcConfiguration.class);
    public static final String PREFIX_CLICKHOUSE = "jdbc:clickhouse:";
    public static final String PREFIX_CLICKHOUSE_SHORT = "jdbc:ch:";

    public static final String USE_SSL_PROP = "ssl";

    final boolean disableFrameworkDetection;

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
        this.disableFrameworkDetection = Boolean.parseBoolean(info.getProperty("disable_frameworks_detection", "false"));
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
        this.isIgnoreUnsupportedRequests= Boolean.parseBoolean(getDriverProperty(DriverProperties.IGNORE_UNSUPPORTED_VALUES.getKey(), "false"));
    }

    public static boolean acceptsURL(String url) {
        // TODO: should be also checked for http/https
        return url.startsWith(PREFIX_CLICKHOUSE) || url.startsWith(PREFIX_CLICKHOUSE_SHORT);
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
     * @param ssl - if SSL protocol should be used when protocol is not specified
     * @return URL without JDBC prefix
     */
    static String createConnectionURL(String url, boolean ssl) throws SQLException {
        if (url.startsWith("//")) {
            url = (ssl ? "https:" : "http:") + url;
        }

        try {
            URI tmp = URI.create(url);
            return tmp.getScheme() + "://" + tmp.getAuthority();
        } catch (Exception e) {
            throw new SQLException("Failed to parse url", e);
        }
    }

    private static String stripJDBCPrefix(String url) {
        if (url.startsWith(PREFIX_CLICKHOUSE)) {
            return url.substring(PREFIX_CLICKHOUSE.length());
        } else if (url.startsWith(PREFIX_CLICKHOUSE_SHORT)) {
            return url.substring(PREFIX_CLICKHOUSE_SHORT.length());
        } else {
            throw new IllegalArgumentException("Specified URL doesn't have jdbc any of prefixes: [ " + PREFIX_CLICKHOUSE + ", " + PREFIX_CLICKHOUSE_SHORT + " ]");
        }
    }

    List<DriverPropertyInfo> listOfProperties;

    /**
     * RegExp that extracts main parts:
     * <ul>
     *     <li>1 - protocol (ex.: {@code http:}) (optional)</li>
     *     <li>2 - host (ex.: {@code localhost} (required)</li>
     *     <li>3 - port (ex.: {@code 8123 } (optional)</li>
     *     <li>4 - database name (optional)</li>
     *     <li>5 - query parameters as is (optional)</li>
     * </ul>
     */
    private static final Pattern URL_REGEXP = Pattern.compile("(https?:)?\\/\\/([\\w\\.\\-]+):?([\\d]*)(?:\\/([\\w]+))?\\/?\\??(.*)$");

    /**
     * Extracts positions of parameters names.
     * Match will be {@code param1=} or {@code &param2=}.
     * There is limitation to not have '=' in values.
     */
    private static final Pattern PARAM_EXTRACT_REGEXP = Pattern.compile("(?:&?[\\w\\.]+)=(?:[\\\\w])*");
    private Map<String, String> parseUrl(String url) throws SQLException {
        Map<String, String> properties = new HashMap<>();

        // process host and protocol
        url = stripJDBCPrefix(url);
        Matcher m = URL_REGEXP.matcher(url);
        if (!m.find()) {
            throw new SQLException("Invalid url " + url);
        }
        String proto = m.group(1);
        String host = m.group(2);
        String port = m.group(3);

        String connectionUrl = (proto == null ? "" : proto)  + "//" + host + (port.isEmpty() ? "" : ":" + port);
        properties.put(PARSE_URL_CONN_URL_PROP, connectionUrl);

        // Set database if present
        String database = m.group(4);
        if (database != null && !database.isEmpty()) {
            properties.put(ClientConfigProperties.DATABASE.getKey(), database);
        }

        // Parse query string
        String queryStr = m.group(5);
        if (queryStr != null && !queryStr.isEmpty()) {
            Matcher qm = PARAM_EXTRACT_REGEXP.matcher(queryStr);

            if (qm.find()) {
                String name = queryStr.substring(qm.start() + (queryStr.charAt(qm.start()) == '&' ? 1 : 0), qm.end() - 1);
                int valStartPos = qm.end();
                while (qm.find()) {
                    String value = queryStr.substring(valStartPos, qm.start());
                    properties.put(name, value);
                    name = queryStr.substring(qm.start() + (queryStr.charAt(qm.start()) == '&' ? 1 : 0), qm.end() - 1);
                    valStartPos = qm.end();
                }

                String value = queryStr.substring(valStartPos);
                properties.put(name, value);
            }
        }

        return properties;
    }

    private static final String PARSE_URL_CONN_URL_PROP = "connection_url";

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

    public Client.Builder applyClientProperties(Client.Builder builder) {
        builder.addEndpoint(connectionUrl)
                .setOptions(clientProperties);
        return builder;
    }

    public void updateUserClient(String clientName, Client client) {
        client.updateClientName((clientName == null || clientName.isEmpty() ? "" : clientName) + ' ' + getDefaultClientName());
    }

    public static String getDefaultClientName() {
        StringBuilder jdbcName = new StringBuilder();
        jdbcName.append(Driver.DRIVER_CLIENT_NAME)
                        .append(Driver.driverVersion);

        return jdbcName.toString();
    }

}
