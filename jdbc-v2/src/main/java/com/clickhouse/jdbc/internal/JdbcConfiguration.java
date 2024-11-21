package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.http.ClickHouseHttpProto;

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
    final String jdbcUrl;
    final boolean disableFrameworkDetection;

    public String getPassword() {
        return password;
    }

    public String getUser() {
        return user;
    }

    public String getUrl() {
        return url;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public boolean isDisableFrameworkDetection() {
        return disableFrameworkDetection;
    }

    public JdbcConfiguration(String url, Properties info) {
        this.jdbcUrl = url;//Raw URL
        this.url = cleanUrl(url);
        this.user = info.getProperty("user", "default");
        this.password = info.getProperty("password", "");
        this.disableFrameworkDetection = Boolean.parseBoolean(info.getProperty("disable_frameworks_detection", "false"));
    }

    public static boolean acceptsURL(String url) {
        return url.startsWith(PREFIX_CLICKHOUSE) || url.startsWith(PREFIX_CLICKHOUSE_SHORT);
    }

    private String cleanUrl(String url) {
        url = stripUrlPrefix(url);
        if (url.startsWith("//")) {
            url = "http:" + url;//Default to HTTP
            try {
                URL parsedUrl = new URL(url);
                if (parsedUrl.getPort() == ClickHouseHttpProto.DEFAULT_HTTPS_PORT) {//If port is 8443, switch to HTTPS
                        url = "https:" + url;
                }
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("URL is not valid.", e);
            }
        }

        return url;
    }
    private String stripUrlPrefix(String url) {
        if (url.startsWith(PREFIX_CLICKHOUSE)) {
            return url.substring(PREFIX_CLICKHOUSE.length());
        } else if (url.startsWith(PREFIX_CLICKHOUSE_SHORT)) {
            return url.substring(PREFIX_CLICKHOUSE_SHORT.length());
        } else {
            throw new IllegalArgumentException("URL is not supported.");
        }
    }
}
