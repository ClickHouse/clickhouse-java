package com.clickhouse.client;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Integration-test target read from {@code TEST_*} environment variables.
 */
final class ClickHouseTestEnvironment {
    static final String CONTAINER = "TEST_CLICKHOUSE_CONTAINER";
    static final String IMAGE = "TEST_CLICKHOUSE_IMAGE";
    static final String IMAGE_VERSION = "TEST_CLICKHOUSE_IMAGE_VERSION";
    static final String HOST = "TEST_CLICKHOUSE_HOST";
    static final String USER = "TEST_CLICKHOUSE_USER";
    static final String PASSWORD = "TEST_CLICKHOUSE_PASSWORD";
    static final String SECURE = "TEST_CLICKHOUSE_SECURE";
    static final String TIMEZONE = "TEST_CLICKHOUSE_TIMEZONE";
    static final String ADDITIONAL_PACKAGES = "TEST_CLICKHOUSE_ADDITIONAL_PACKAGES";
    static final String CONTAINER_ID = "TEST_CLICKHOUSE_CONTAINER_ID";
    static final String DATABASE = "TEST_DB_NAME";
    static final String PROXY_ADDRESS = "TEST_PROXY_ADDRESS";
    static final String PROXY_IMAGE = "TEST_PROXY_IMAGE";

    static final String DEFAULT_IMAGE = "clickhouse/clickhouse-server";
    static final String DEFAULT_HOST = "localhost";
    static final String DEFAULT_USER = "default";
    static final String DEFAULT_PASSWORD = "test_default_password";
    static final String DEFAULT_TIMEZONE = "UTC";
    static final String DEFAULT_PROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.5.0";

    enum ContainerMode {
        SINGLE, CLUSTER, NONE
    }

    private final Map<String, String> env;
    private final ContainerMode containerMode;
    private final String imageRef;
    private final String clickHouseVersion;
    private final String additionalPackages;
    private final String host;
    private final String user;
    private final String password;
    private final boolean secure;
    private final String timezone;
    private final String containerName;
    private final String database;
    private final boolean localDatabase;
    private final String proxyHost;
    private final int proxyPort;
    private final String proxyImage;

    private ClickHouseTestEnvironment(Map<String, String> env, ContainerMode containerMode, String imageRef,
            String clickHouseVersion, String additionalPackages, String host, String user, String password,
            boolean secure, String timezone, String containerName, String database, boolean localDatabase,
            String proxyHost, int proxyPort, String proxyImage) {
        this.env = env;
        this.containerMode = containerMode;
        this.imageRef = imageRef;
        this.clickHouseVersion = clickHouseVersion;
        this.additionalPackages = additionalPackages;
        this.host = host;
        this.user = user;
        this.password = password;
        this.secure = secure;
        this.timezone = timezone;
        this.containerName = containerName;
        this.database = database;
        this.localDatabase = localDatabase;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyImage = proxyImage;
    }

    static final String REMOVED_VERSION_PROPERTY = "clickhouseVersion";

    /**
     * Fails the run when the removed {@code clickhouseVersion} system property is set.
     * Image selection is {@code TEST_CLICKHOUSE_IMAGE_VERSION}.
     */
    static void rejectRemovedOptions(Properties properties) {
        if (properties == null) {
            return;
        }
        String version = properties.getProperty(REMOVED_VERSION_PROPERTY);
        if (version != null && !version.trim().isEmpty()) {
            throw new IllegalArgumentException("System property clickhouseVersion is no longer supported"
                    + " (was '" + version.trim() + "'). Set TEST_CLICKHOUSE_IMAGE_VERSION to select the"
                    + " container image.");
        }
    }

    static ClickHouseTestEnvironment from(Map<String, String> env) {
        Map<String, String> source = new HashMap<String, String>();
        if (env != null) {
            source.putAll(env);
        }

        ContainerMode containerMode = parseMode(value(source, CONTAINER));

        String imageName = value(source, IMAGE);
        if (imageName == null) {
            imageName = DEFAULT_IMAGE;
        }
        String requestedVersion = value(source, IMAGE_VERSION);
        String clickHouseVersion;
        String imageRef;
        int tagIndex = imageName.indexOf(':');
        int digestIndex = imageName.indexOf('@');
        if (digestIndex > 0 && (tagIndex < 0 || digestIndex < tagIndex)) {
            clickHouseVersion = "";
            imageRef = imageName;
        } else if (tagIndex > 0) {
            clickHouseVersion = digestIndex > tagIndex ? imageName.substring(tagIndex + 1, digestIndex)
                    : imageName.substring(tagIndex + 1);
            imageRef = imageName;
        } else if (requestedVersion == null) {
            clickHouseVersion = "";
            imageRef = imageName;
        } else {
            clickHouseVersion = ClickHouseVersionUtils.of(requestedVersion).getYear() == 0 ? "" : requestedVersion;
            imageRef = imageName + ":" + requestedVersion;
        }

        String additionalPackages = value(source, ADDITIONAL_PACKAGES);
        if (ClickHouseVersionUtils.check(clickHouseVersion, "(,21.3]")) {
            if (additionalPackages == null) {
                additionalPackages = "tzdata";
            } else if (!additionalPackages.contains("tzdata")) {
                additionalPackages = additionalPackages + " tzdata";
            }
        }

        String externalDatabase = value(source, DATABASE);
        final String database;
        final boolean localDatabase;
        if (externalDatabase != null) {
            if (!externalDatabase.startsWith("clickhouse_java_test_")) {
                throw new RuntimeException("external database for tests should start with 'clickhouse_java_test_'");
            }
            localDatabase = false;
            database = externalDatabase;
        } else {
            localDatabase = true;
            database = "clickhouse_java_" + UUID.randomUUID().toString().substring(0, 8) + "_test_"
                    + System.currentTimeMillis();
        }

        String proxy = value(source, PROXY_ADDRESS);
        final String proxyHost;
        final int proxyPort;
        final String proxyImage;
        if (proxy != null) {
            int index = proxy.indexOf(':');
            if (index > 0) {
                proxyHost = proxy.substring(0, index);
                proxyPort = Integer.parseInt(proxy.substring(index + 1));
            } else {
                proxyHost = proxy;
                proxyPort = 8666;
            }
            proxyImage = "";
        } else {
            proxyHost = "";
            proxyPort = -1;
            String image = value(source, PROXY_IMAGE);
            proxyImage = image == null ? DEFAULT_PROXY_IMAGE : image;
        }

        String host = value(source, HOST);
        String user = value(source, USER);
        String password = value(source, PASSWORD);
        String timezone = value(source, TIMEZONE);

        return new ClickHouseTestEnvironment(Collections.unmodifiableMap(source), containerMode, imageRef,
                clickHouseVersion, additionalPackages,
                host == null ? DEFAULT_HOST : host,
                user == null ? DEFAULT_USER : user,
                password == null ? DEFAULT_PASSWORD : password,
                parseSecure(value(source, SECURE)),
                timezone == null ? DEFAULT_TIMEZONE : timezone,
                value(source, CONTAINER_ID),
                database, localDatabase, proxyHost, proxyPort, proxyImage);
    }

    String portOverride(ClickHouseProtocol protocol) {
        return value(env, "TEST_CLICKHOUSE_" + protocol.name() + "_PORT");
    }

    ContainerMode getContainerMode() {
        return containerMode;
    }

    String getImageRef() {
        return imageRef;
    }

    String getClickHouseVersion() {
        return clickHouseVersion;
    }

    String getAdditionalPackages() {
        return additionalPackages;
    }

    String getHost() {
        return host;
    }

    String getUser() {
        return user;
    }

    String getPassword() {
        return password;
    }

    boolean isSecure() {
        return secure;
    }

    String getTimezone() {
        return timezone;
    }

    String getContainerName() {
        return containerName;
    }

    String getDatabase() {
        return database;
    }

    boolean isLocalDatabase() {
        return localDatabase;
    }

    String getProxyHost() {
        return proxyHost;
    }

    int getProxyPort() {
        return proxyPort;
    }

    String getProxyImage() {
        return proxyImage;
    }

    private static ContainerMode parseMode(String raw) {
        if (raw == null) {
            return ContainerMode.SINGLE;
        }
        if ("single".equalsIgnoreCase(raw)) {
            return ContainerMode.SINGLE;
        }
        if ("cluster".equalsIgnoreCase(raw)) {
            return ContainerMode.CLUSTER;
        }
        if ("none".equalsIgnoreCase(raw)) {
            return ContainerMode.NONE;
        }
        throw new IllegalArgumentException(
                "TEST_CLICKHOUSE_CONTAINER must be single, cluster, or none, but was '" + raw + "'");
    }

    private static boolean parseSecure(String raw) {
        if (raw == null) {
            return false;
        }
        if ("true".equalsIgnoreCase(raw) || "1".equals(raw)) {
            return true;
        }
        if ("false".equalsIgnoreCase(raw) || "0".equals(raw)) {
            return false;
        }
        throw new IllegalArgumentException(
                "TEST_CLICKHOUSE_SECURE must be true or false, but was '" + raw + "'");
    }

    private static String value(Map<String, String> env, String key) {
        String raw = env.get(key);
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
