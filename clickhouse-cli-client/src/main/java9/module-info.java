module com.clickhouse.client.cli {
    exports com.clickhouse.client.cli;
    exports com.clickhouse.client.cli.config;

    provides com.clickhouse.client.ClickHouseClient with com.clickhouse.client.cli.ClickHouseCommandLineClient;

    requires transitive com.clickhouse.client;
}
