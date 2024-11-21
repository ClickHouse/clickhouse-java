package com.clickhouse.jdbc.internal;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.DriverPropertyInfo;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class JdbcConfiguration {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JdbcConfiguration.class);
    public static final String PREFIX_CLICKHOUSE = "jdbc:clickhouse:";
    public static final String PREFIX_CLICKHOUSE_SHORT = "jdbc:ch:";

    final String user;
    final String password;
    final String url;

    public String getPassword() {
        return password;
    }

    public String getUser() {
        return user;
    }

    public String getUrl() {
        return url;
    }

    public JdbcConfiguration(String url, Properties info) {
        this.url = stripUrlPrefix(url);
        this.user = info.getProperty("user", "default");
        this.password = info.getProperty("password", "");
    }

    public static boolean acceptsURL(String url) {
        return url.startsWith(PREFIX_CLICKHOUSE) || url.startsWith(PREFIX_CLICKHOUSE_SHORT);
    }

    public String stripUrlPrefix(String url) {
        if (url.startsWith(PREFIX_CLICKHOUSE)) {
            return url.substring(PREFIX_CLICKHOUSE.length());
        } else if (url.startsWith(PREFIX_CLICKHOUSE_SHORT)) {
            return url.substring(PREFIX_CLICKHOUSE_SHORT.length());
        } else {
            throw new IllegalArgumentException("URL is not supported.");
        }
    }
}
