package com.clickhouse.jdbc.internal;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class JdbcConfiguration {
    final String host;
    final int port;
    final String protocol;
    final String database;
    final String user;
    final String password;
    final Map<String, String> queryParams;

    public String getDatabase() {
        return database;
    }

    public String getHost() {
        return host;
    }

    public String getPassword() {
        return password;
    }

    public int getPort() {
        return port;
    }

    public String getProtocol() {
        return protocol;
    }

    public Map<String, String> getQueryParams() {
        return queryParams;
    }

    public String getUser() {
        return user;
    }

    public JdbcConfiguration(String url, Properties info) {
        Map<String, String> urlProperties = parseUrl(url);
        this.host = urlProperties.get("host");
        this.port = Integer.parseInt(urlProperties.get("port"));
        this.protocol = urlProperties.get("protocol");
        this.database = urlProperties.get("database") == null ? "default" : urlProperties.get("database");
        this.queryParams = urlProperties.get("queryParams") == null ? new HashMap<>() : parseQueryParams(urlProperties.get("queryParams"));


        this.user = info.getProperty("user", "default");
        this.password = info.getProperty("password", "");
    }

    private Map<String, String> parseUrl(String urlString) {
        URL url;
        try {
            url = new URL(stripUrlPrefix(urlString));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("URL is malformed.");
        }

        Map<String, String> urlProperties = new HashMap<>();
        urlProperties.put("host", url.getHost());
        urlProperties.put("port", String.valueOf(url.getPort() == -1 ? 8443 : url.getPort()));
        urlProperties.put("protocol", url.getProtocol());
        urlProperties.put("database", url.getPath().substring(1));
        urlProperties.put("queryParams", url.getQuery());

        return urlProperties;
    }
    private String stripUrlPrefix(String url) {
        if (url.startsWith("jdbc:clickhouse:")) {
            return url.substring("jdbc:clickhouse:".length());
        } else if (url.startsWith("jdbc:ch:")) {
            return url.substring("jdbc:ch:".length());
        } else {
            throw new IllegalArgumentException("URL is not supported.");
        }
    }
    private Map<String, String> parseQueryParams(String queryParams) {
        if (queryParams == null || queryParams.isEmpty()) {
            return new HashMap<>(0);
        }

        return Arrays.stream(queryParams.split("&"))
                .map(s -> {
                            String[] parts = s.split("=");
                            return new AbstractMap.SimpleImmutableEntry<>(parts[0], parts[1]);
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
