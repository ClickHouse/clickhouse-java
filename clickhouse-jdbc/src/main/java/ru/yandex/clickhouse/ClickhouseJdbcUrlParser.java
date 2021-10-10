package ru.yandex.clickhouse;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

public class ClickhouseJdbcUrlParser {
    private static final Logger log = LoggerFactory.getLogger(ClickhouseJdbcUrlParser.class);
    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBC_CLICKHOUSE_PREFIX = JDBC_PREFIX + "clickhouse:";
    public static final Pattern DB_PATH_PATTERN = Pattern.compile("/([a-zA-Z0-9_*\\-]+)");
    protected final static String DEFAULT_DATABASE = "default";

    private ClickhouseJdbcUrlParser(){
    }

    public static ClickHouseProperties parse(String jdbcUrl, Properties defaults) throws URISyntaxException
    {
        if (!jdbcUrl.startsWith(JDBC_CLICKHOUSE_PREFIX)) {
            throw new URISyntaxException(jdbcUrl, "'" + JDBC_CLICKHOUSE_PREFIX + "' prefix is mandatory");
        }
        return parseClickhouseUrl(jdbcUrl.substring(JDBC_PREFIX.length()), defaults);
    }

    private static ClickHouseProperties parseClickhouseUrl(String uriString, Properties defaults)
            throws URISyntaxException
    {
        URI uri = new URI(uriString);
        Properties urlProperties = parseUriQueryPart(uri.getQuery(), defaults);
        ClickHouseProperties props = new ClickHouseProperties(urlProperties);
        props.setHost(uri.getHost());
        int port = uri.getPort();
        if (port == -1) {
            throw new IllegalArgumentException("port is missed or wrong");
        }
        props.setPort(port);
        String path = uri.getPath();
        String database;
        if (props.isUsePathAsDb()) {
            if (path == null || path.isEmpty() || path.equals("/")) {
                String defaultsDb = defaults.getProperty(ClickHouseQueryParam.DATABASE.getKey());
                database = defaultsDb == null ? DEFAULT_DATABASE : defaultsDb;
            } else {
                Matcher m = DB_PATH_PATTERN.matcher(path);
                if (m.matches()) {
                    database = m.group(1);
                } else {
                    throw new URISyntaxException("wrong database name path: '" + path + "'", uriString);
                }
            }
            props.setDatabase(database);
        } else {
            if (props.getDatabase() == null || props.getDatabase().isEmpty()) {
                props.setDatabase(DEFAULT_DATABASE);
            }
            if (path == null || path.isEmpty()) {
                props.setPath("/");
            } else {
                props.setPath(path);
            }
        }
        return props;
    }

    static Properties parseUriQueryPart(String query, Properties defaults) {
        if (query == null) {
            return defaults;
        }
        Properties urlProps = new Properties(defaults);
        String queryKeyValues[] = query.split("&");
        for (String keyValue : queryKeyValues) {
            String keyValueTokens[] = keyValue.split("=");
            if (keyValueTokens.length == 2) {
                urlProps.put(keyValueTokens[0], keyValueTokens[1]);
            } else {
                log.warn("don't know how to handle parameter pair: %s", keyValue);
            }
        }
        return urlProps;
    }
}
