package com.clickhouse.benchmark.jdbc;

import com.clickhouse.benchmark.Constants;

public enum JdbcDriver {
    // ClickHouse JDBC Driver
    ClickhouseJdbc("com.clickhouse.jdbc.ClickHouseDriver",
            "jdbc:ch://%s:%s/%s?http_connection_provider=HTTP_URL_CONNECTION&ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s%s",
            Constants.HTTP_PORT),
    // default http implementation
    ClickhouseHttpJdbc("com.clickhouse.jdbc.ClickHouseDriver",
            "jdbc:ch://%s:%s/%s?http_connection_provider=HTTP_URL_CONNECTION&ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s%s",
            Constants.HTTP_PORT),
    ClickhouseHttpUrlConnectionJdbc("com.clickhouse.jdbc.ClickHouseDriver",
            "jdbc:ch://%s:%s/%s?http_connection_provider=HTTP_URL_CONNECTION&ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s%s",
            Constants.HTTP_PORT),
    ClickhouseHttpClientJdbc("com.clickhouse.jdbc.ClickHouseDriver",
            "jdbc:ch://%s:%s/%s?http_connection_provider=HTTP_CLIENT&ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s%s",
            Constants.HTTP_PORT),
    ClickhouseApacheHttpClientJdbc("com.clickhouse.jdbc.ClickHouseDriver",
            "jdbc:ch://%s:%s/%s?http_connection_provider=APACHE_HTTP_CLIENT&ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s%s",
            Constants.HTTP_PORT),
    // default gRPC implementation
    ClickhouseGrpcJdbc("com.clickhouse.jdbc.ClickHouseDriver",
            "jdbc:ch:grpc://%s:%s/%s?ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&max_inbound_message_size=2147483647&compress=%s%s",
            Constants.GRPC_PORT),
    ClickhouseGrpcNettyJdbc("com.clickhouse.jdbc.ClickHouseDriver",
            "jdbc:ch:grpc://%s:%s/%s?ssl=false&use_okhttp=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&max_inbound_message_size=2147483647&compress=%s%s",
            Constants.GRPC_PORT),
    ClickhouseGrpcOkHttpJdbc("com.clickhouse.jdbc.ClickHouseDriver",
            "jdbc:ch:grpc://%s:%s/%s?ssl=false&use_okhttp=true&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&max_inbound_message_size=2147483647&compress=%s%s",
            Constants.GRPC_PORT),
    // version prior to 0.3.2
    ClickhouseLegacyJdbc("ru.yandex.clickhouse.ClickHouseDriver",
            "jdbc:clickhouse://%s:%s/%s?ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s%s",
            Constants.HTTP_PORT),
    // ClickHouse4j
    Clickhouse4j("cc.blynk.clickhouse.ClickHouseDriver",
            "jdbc:clickhouse://%s:%s/%s?ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s%s",
            Constants.HTTP_PORT),
    // ClickHouse Native JDBC Driver
    ClickhouseNativeJdbc("com.github.housepower.jdbc.ClickHouseDriver",
            "jdbc:clickhouse://%s:%s/%s?ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s%s",
            Constants.NATIVE_PORT),
    // MariaDB Java Client
    MariadbJavaClient("org.mariadb.jdbc.Driver",
            "jdbc:mariadb://%s:%s/%s?user=%s&password=%s&useSSL=false&useServerPrepStmts=false&useCompression=%s"
                    + "&rewriteBatchedStatements=true&cachePrepStmts=true&serverTimezone=UTC%s",
            Constants.MYSQL_PORT),
    // MySQL Connector/J
    MysqlConnectorJava("com.mysql.cj.jdbc.Driver",
            "jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&useServerPrepStmts=false"
                    + "&rewriteBatchedStatements=true&cachePrepStmts=true&connectionTimeZone=UTC&useCompression=%S%s",
            Constants.MYSQL_PORT),
    // PostgreSQL JDBC Driver
    PostgresqlJdbc("org.postgresql.Driver",
            "jdbc:postgresql://%s:%s/%s?user=%s&password=%s&ssl=false&sslmode=disable&preferQueryMode=simple&compress=%s%s",
            Constants.POSTGRESQL_PORT);

    private final String className;
    private final String urlTemplate;
    private final int defaultPort;

    public static JdbcDriver from(String driver) {
        if (driver == null || driver.isEmpty()) {
            throw new IllegalArgumentException("Non-empty driver is needed");
        }

        String[] parts = driver.split(" ");
        if (parts.length > 2) {
            throw new IllegalArgumentException("Only format '<name> [version]' is supported!");
        }

        String name = parts[0].replace("-", "");

        for (JdbcDriver d : JdbcDriver.values()) {
            if (d.name().equalsIgnoreCase(name)) {
                return d;
            }
        }

        throw new IllegalArgumentException("Unsupported driver: " + name);
    }

    JdbcDriver(String className, String urlTemplate, int defaultPort) {
        this.className = className;
        this.urlTemplate = urlTemplate;
        this.defaultPort = defaultPort;
    }

    public String getClassName() {
        return this.className;
    }

    public String getUrlTemplate() {
        return this.urlTemplate;
    }

    public int getDefaultPort() {
        return this.defaultPort;
    }
}
