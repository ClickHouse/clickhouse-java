package ru.yandex.clickhouse;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

public class ClickhouseJdbcUrlParser {
    private static final Logger logger = LoggerFactory.getLogger(ClickhouseJdbcUrlParser.class);
    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBC_CLICKHOUSE_PREFIX = JDBC_PREFIX + "clickhouse:";
    public static final Pattern DB_PATH_PATTERN = Pattern.compile("/([a-zA-Z0-9_\\*\\-]+)");
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
        Properties urlProperties = parseUriQueryPart(uri, defaults);
        ClickHouseProperties props = new ClickHouseProperties(urlProperties);
        props.setHost(uri.getHost());
        int port = uri.getPort();
        if (port == -1) {
            throw new IllegalArgumentException("port is missed or wrong");
        }
        props.setPort(port);
        String database = uri.getPath();
        if (database == null || database.isEmpty() || "/".equals(database)) {
            String defaultsDb = defaults.getProperty(ClickHouseQueryParam.DATABASE.getKey());
            database = defaultsDb == null ? DEFAULT_DATABASE : defaultsDb;
        } else {
            Matcher m = DB_PATH_PATTERN.matcher(database);
            if (m.matches()) {
                database = m.group(1);
            } else {
                throw new URISyntaxException("wrong database name path: '" + database + "'", uriString);
            }
        }
        props.setDatabase(database);
        return props;
    }

    private static Properties parseUriQueryPart(URI uri, Properties defaults) {
        String query = uri.getQuery();
        if (query == null) {
            return defaults;
        }
        Properties urlProps = new Properties(defaults);
        String queryKeyVaues[] = query.split("&");
        for (String keyValue : queryKeyVaues) {
            String keyValueTokens[] = keyValue.split("=");
            if (keyValueTokens.length == 2) {
                urlProps.put(keyValueTokens[0], keyValueTokens[1]);
            } else {
                logger.warn("don't know how to handle parameter pair: {}", keyValue);
            }
        }
        return urlProps;
    }
}
