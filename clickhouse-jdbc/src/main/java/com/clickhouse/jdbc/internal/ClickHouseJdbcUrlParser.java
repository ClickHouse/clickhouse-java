package com.clickhouse.jdbc.internal;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodes;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.jdbc.JdbcConfig;
import com.clickhouse.jdbc.SqlExceptionUtils;

@Deprecated
public class ClickHouseJdbcUrlParser {
    public static class ConnectionInfo {
        private final String cacheKey;
        private final ClickHouseCredentials credentials;
        private final ClickHouseNodes nodes;
        private final JdbcConfig jdbcConf;
        private final Properties props;

        protected ConnectionInfo(String cacheKey, ClickHouseNodes nodes, Properties props) {
            this.cacheKey = cacheKey;
            this.nodes = nodes;
            this.jdbcConf = new JdbcConfig(props);
            this.props = props;

            ClickHouseCredentials c = nodes.getTemplate().getCredentials().orElse(null);
            if (props != null && !props.isEmpty()) {
                String user = props.getProperty(ClickHouseDefaults.USER.getKey(), "");
                String passwd = props.getProperty(ClickHouseDefaults.PASSWORD.getKey(), "");
                if (!ClickHouseChecker.isNullOrEmpty(user)) {
                    c = ClickHouseCredentials.fromUserAndPassword(user, passwd);
                }
            }
            this.credentials = c;
        }

        public ClickHouseCredentials getDefaultCredentials() {
            return this.credentials;
        }

        /**
         * Gets selected server.
         *
         * @return non-null selected server
         * @deprecated will be removed in 0.5, please use {@link #getNodes()}
         *             instead
         */
        @Deprecated
        public ClickHouseNode getServer() {
            return nodes.apply(nodes.getNodeSelector());
        }

        public JdbcConfig getJdbcConfig() {
            return jdbcConf;
        }

        /**
         * Gets nodes defined in connection string.
         *
         * @return non-null nodes
         */
        public ClickHouseNodes getNodes() {
            return nodes;
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

    static Properties newProperties() {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.ASYNC.getKey(), Boolean.FALSE.toString());
        props.setProperty(ClickHouseClientOption.FORMAT.getKey(), ClickHouseFormat.RowBinaryWithNamesAndTypes.name());
        props.setProperty(ClickHouseClientOption.PRODUCT_NAME.getKey(), "ClickHouse-JdbcDriver");
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
            String cacheKey = ClickHouseNodes.buildCacheKey(jdbcUrl, defaults);
            ClickHouseNodes nodes = ClickHouseNodes.of(cacheKey, jdbcUrl, defaults);
            Properties props = newProperties();
            props.putAll(nodes.getTemplate().getOptions());
            props.putAll(defaults);
            return new ConnectionInfo(cacheKey, nodes, props);
        } catch (IllegalArgumentException e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }

    private ClickHouseJdbcUrlParser() {
    }
}
