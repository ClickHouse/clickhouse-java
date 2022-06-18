package com.clickhouse.jdbc.internal;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodes;
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
        private final ClickHouseCredentials credentials;
        private final ClickHouseNodes nodes;
        private final JdbcConfig jdbcConf;
        private final Properties props;

        protected ConnectionInfo(ClickHouseNodes nodes, Properties props) {
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

        public ClickHouseNode getServer() {
            return nodes.apply(nodes.getNodeSelector());
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
            ClickHouseNodes nodes = ClickHouseNodes.of(jdbcUrl, defaults);
            Properties props = newProperties();
            props.putAll(nodes.getTemplate().getOptions());
            return new ConnectionInfo(nodes, props);
        } catch (IllegalArgumentException e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }

    private ClickHouseJdbcUrlParser() {
    }
}
