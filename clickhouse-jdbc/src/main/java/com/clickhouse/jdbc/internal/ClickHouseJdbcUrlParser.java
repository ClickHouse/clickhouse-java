package com.clickhouse.jdbc.internal;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import com.clickhouse.jdbc.JdbcConfig;
import com.clickhouse.jdbc.SqlExceptionUtils;

public class ClickHouseJdbcUrlParser {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseJdbcUrlParser.class);

    public static class ConnectionInfo {
        private final URI uri;
        private final ClickHouseNode server;
        private final JdbcConfig jdbcConf;
        private final Properties props;

        protected ConnectionInfo(URI uri, ClickHouseNode server, Properties props) throws URISyntaxException {
            this.uri = new URI("jdbc:clickhouse:" + server.getProtocol().name().toLowerCase(Locale.ROOT), null,
                    server.getHost(), server.getPort(), "/" + server.getDatabase().orElse(""),
                    removeCredentialsFromQuery(uri.getRawQuery()), null);
            this.server = server;
            this.jdbcConf = new JdbcConfig(props);
            this.props = props;
        }

        public URI getUri() {
            return uri;
        }

        public ClickHouseNode getServer() {
            return server;
        }

        public JdbcConfig getJdbcConfig() {
            return jdbcConf;
        }

        public Properties getProperties() {
            return props;
        }
    }

    // URL pattern:
    // jdbc:(clickhouse|ch)[:(grpc|http|tcp)]://host[:port][/db][?param1=value1&param2=value2]
    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBC_CLICKHOUSE_PREFIX = JDBC_PREFIX + "clickhouse:";
    public static final String JDBC_ABBREVIATION_PREFIX = JDBC_PREFIX + "ch:";

    private static String decode(String str) {
        try {
            return URLDecoder.decode(str, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // don't print the content here as it may contain password
            log.warn("Failed to decode given string, fallback to the original string");
            return str;
        }
    }

    private static ClickHouseNode parseNode(URI uri, Properties defaults) {
        ClickHouseProtocol protocol = ClickHouseProtocol.fromUriScheme(uri.getScheme());
        if (protocol == null || protocol == ClickHouseProtocol.ANY) {
            throw new IllegalArgumentException("Unsupported scheme: " + uri.getScheme());
        }

        String host = uri.getHost();
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Host is missed or wrong");
        }

        int port = uri.getPort();
        if (port == -1) {
            port = protocol.getDefaultPort();
        }

        String userName = defaults.getProperty(ClickHouseDefaults.USER.getKey());
        String password = defaults.getProperty(ClickHouseDefaults.PASSWORD.getKey());
        String userInfo = uri.getRawUserInfo();
        if (userInfo != null && !userInfo.isEmpty()) {
            int index = userInfo.indexOf(':');
            if (userName == null) {
                userName = index == 0 ? userName : decode(index > 0 ? userInfo.substring(0, index) : userInfo);
            }
            if (password == null) {
                password = index < 0 ? password : decode(userInfo.substring(index + 1));
            }
        }
        if (userName == null || userName.isEmpty()) {
            userName = (String) ClickHouseDefaults.USER.getEffectiveDefaultValue();
        }
        if (password == null) {
            password = (String) ClickHouseDefaults.PASSWORD.getEffectiveDefaultValue();
        }

        final String dbKey = ClickHouseDefaults.DATABASE.getKey();

        String path = uri.getPath();
        String database;
        if (path == null || path.isEmpty() || path.equals("/")) {
            database = defaults.getProperty(dbKey);
        } else if (path.charAt(0) == '/') {
            database = defaults.getProperty(dbKey, path.substring(1));
        } else {
            throw new IllegalArgumentException("wrong database name path: '" + path + "'");
        }
        if (database == null || database.isEmpty()) {
            database = (String) ClickHouseDefaults.DATABASE.getEffectiveDefaultValue();
        }

        defaults.setProperty(dbKey, database);

        return ClickHouseNode.builder().host(host).port(protocol, port).database(database)
                .credentials(ClickHouseCredentials.fromUserAndPassword(userName, password)).build();
    }

    private static Properties parseParams(String query, Properties props) {
        if (query == null || query.isEmpty()) {
            return props;
        }

        String[] queryKeyValues = query.split("&");
        for (String keyValue : queryKeyValues) {
            int index = keyValue.indexOf('=');
            if (index <= 0) {
                log.warn("don't know how to handle parameter pair: %s", keyValue);
                continue;
            }

            props.put(decode(keyValue.substring(0, index)), decode(keyValue.substring(index + 1)));
        }
        return props;
    }

    static String removeCredentialsFromQuery(String query) {
        if (query == null || query.isEmpty()) {
            return null;
        }

        StringBuilder builder = new StringBuilder(query.length());
        int start = 0;
        int index = 0;
        do {
            index = query.indexOf('&', start);
            String kv = query.substring(start, index < 0 ? query.length() : index);
            start += kv.length() + 1;
            int i = kv.indexOf('=');
            if (i > 0) {
                String key = kv.substring(0, i);
                if (!ClickHouseDefaults.USER.getKey().equals(key)
                        && !ClickHouseDefaults.PASSWORD.getKey().equals(key)) {
                    builder.append(kv).append('&');
                }
            }
        } while (index >= 0);

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
            query = builder.toString();
        } else {
            query = null;
        }

        return query;
    }

    static Properties newProperties() {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.ASYNC.getKey(), Boolean.FALSE.toString());
        props.setProperty(ClickHouseClientOption.FORMAT.getKey(), ClickHouseFormat.RowBinaryWithNamesAndTypes.name());
        return props;
    }

    public static ConnectionInfo parse(String jdbcUrl, Properties defaults) throws SQLException {
        if (defaults == null) {
            defaults = new Properties();
        }

        if (ClickHouseChecker.isNullOrBlank(jdbcUrl)) {
            throw SqlExceptionUtils.clientError("Non-blank JDBC URL is required");
        }

        if (jdbcUrl.startsWith(JDBC_CLICKHOUSE_PREFIX)) {
            jdbcUrl = jdbcUrl.substring(JDBC_CLICKHOUSE_PREFIX.length());
        } else if (jdbcUrl.startsWith(JDBC_ABBREVIATION_PREFIX)) {
            jdbcUrl = jdbcUrl.substring(JDBC_ABBREVIATION_PREFIX.length());
        } else {
            throw SqlExceptionUtils.clientError(
                    new URISyntaxException(jdbcUrl, ClickHouseUtils.format("'%s' or '%s' prefix is mandatory",
                            JDBC_CLICKHOUSE_PREFIX, JDBC_ABBREVIATION_PREFIX)));
        }

        int index = jdbcUrl.indexOf("//");
        if (index == -1) {
            throw SqlExceptionUtils
                    .clientError(new URISyntaxException(jdbcUrl, "Missing '//' from the given JDBC URL"));
        } else if (index == 0) {
            jdbcUrl = "http:" + jdbcUrl;
        }

        try {
            URI uri = new URI(jdbcUrl);
            Properties props = newProperties();
            props.putAll(defaults);
            parseParams(uri.getQuery(), props);
            return new ConnectionInfo(uri, parseNode(uri, props), props);
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }

    private ClickHouseJdbcUrlParser() {
    }
}
