package com.clickhouse.jdbc.internal;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.jdbc.Driver;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JdbcConfiguration {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JdbcConfiguration.class);
    public static final String PREFIX_CLICKHOUSE = "jdbc:clickhouse:";
    public static final String PREFIX_CLICKHOUSE_SHORT = "jdbc:ch:";

    public static final String USE_SSL_PROP = "ssl";

    final boolean disableFrameworkDetection;

    private final Map<String, String> clientProperties;

    private final Map<String, String> driverProperties;

    private final String connectionUrl;

    public boolean isDisableFrameworkDetection() {
        return disableFrameworkDetection;
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
        initProperties(stripUrlPrefix(url), info);

        boolean useSSL = Boolean.parseBoolean(info.getProperty("ssl", "false"));
        this.connectionUrl = createConnectionURL(url, useSSL);
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
        url = stripUrlPrefix(url);
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

    private static String stripUrlPrefix(String url) {
        if (url.startsWith(PREFIX_CLICKHOUSE)) {
            return url.substring(PREFIX_CLICKHOUSE.length());
        } else if (url.startsWith(PREFIX_CLICKHOUSE_SHORT)) {
            return url.substring(PREFIX_CLICKHOUSE_SHORT.length());
        } else {
            throw new IllegalArgumentException("Specified URL doesn't have jdbc any of prefixes: [ " + PREFIX_CLICKHOUSE + ", " + PREFIX_CLICKHOUSE_SHORT + " ]");
        }
    }

    List<DriverPropertyInfo> listOfProperties;

    private void initProperties(String url, Properties providedProperties) {

        // Parse url for database name and override
        try {
            URI tmp = new URI(url);
            String path = tmp.getPath();
            if (path != null) {
                String[] pathElements = path.split("([\\/]+)+", 3);
                if (pathElements.length > 2) {
                    throw new IllegalArgumentException("There can be only one URL path element indicating a database name");
                } else if (pathElements.length == 2) {
                    providedProperties.put(ClientConfigProperties.DATABASE.getKey(), pathElements[1]);
                }
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid JDBC URL is specified");
        }

        // Process properties
        Map<String, String> props = new HashMap<>();
        for (Map.Entry<Object, Object> entry : providedProperties.entrySet()) {
            if (entry.getKey() instanceof String && entry.getValue() instanceof String) {
                props.put((String) entry.getKey(), (String) entry.getValue());
            } else {
                throw new IllegalArgumentException("Property key and value should be a string");
            }
        }

        Map<String, DriverPropertyInfo> propertyInfos = new HashMap<>();
        // create initial list of properties that will be passed to a client
        for (Map.Entry<String, String> prop : ClickHouseUtils.extractParameters(url, props).entrySet()) {
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

        listOfProperties = propertyInfos.values().stream().sorted(Comparator.comparing(o -> o.name)).toList();
    }

    /**
     * Returns a list of driver property information.
     * @return a list of driver property information for the driver
     */
    public List<DriverPropertyInfo> getDriverPropertyInfo() {
        return listOfProperties;
    }

    public Client.Builder applyClientProperties(Client.Builder builder) {
        builder.addEndpoint(connectionUrl)
                .setOptions(clientProperties);

        return builder;
    }
}
