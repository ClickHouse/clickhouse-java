package com.clickhouse.benchmark.jdbc;

import com.clickhouse.benchmark.Constants;

public enum JdbcDriver {
    // ClickHouse4j
    Clickhouse4j("cc.blynk.clickhouse.ClickHouseDriver",
            "jdbc:clickhouse://%s:%s/%s?ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s",
            Constants.HTTP_PORT),
    // ClickHouse JDBC Driver
    ClickhouseHttpJdbc("com.clickhouse.jdbc.ClickHouseDriver",
            "jdbc:ch://%s:%s/%s?ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s",
            Constants.HTTP_PORT),
    ClickhouseGrpcJdbc("com.clickhouse.jdbc.ClickHouseDriver",
            "jdbc:ch:grpc://%s:%s/%s?ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s",
            Constants.GRPC_PORT),
    ClickhouseJdbc("ru.yandex.clickhouse.ClickHouseDriver",
            "jdbc:clickhouse://%s:%s/%s?ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s",
            Constants.HTTP_PORT),
    // ClickHouse Native JDBC Driver
    ClickhouseNativeJdbcShaded("com.github.housepower.jdbc.ClickHouseDriver",
            "jdbc:clickhouse://%s:%s/%s?ssl=false&user=%s&password=%s&use_server_time_zone=false&use_time_zone=UTC&compress=%s",
            Constants.NATIVE_PORT),

    // MariaDB Java Client
    MariadbJavaClient("org.mariadb.jdbc.Driver",
            "jdbc:mariadb://%s:%s/%s?user=%s&password=%s&useSSL=false&useServerPrepStmts=false&useCompression=%s"
                    + "&rewriteBatchedStatements=true&cachePrepStmts=true&serverTimezone=UTC",
            Constants.MYSQL_PORT),

    // MySQL Connector/J
    MysqlConnectorJava("com.mysql.cj.jdbc.Driver",
            "jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&useServerPrepStmts=false"
                    + "&rewriteBatchedStatements=true&cachePrepStmts=true&connectionTimeZone=UTC&useCompression=%s",
            Constants.MYSQL_PORT),

    // PostgreSQL JDBC Driver
    PostgresqlJdbc("org.postgresql.Driver",
            "jdbc:postgresql://%s:%s/%s?user=%s&password=%s&ssl=false&sslmode=disable&preferQueryMode=simple&compress=%s",
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
