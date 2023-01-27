/**
 * Declares com.clickhouse module.
 */
module com.clickhouse.jdbc {
    exports com.clickhouse.jdbc;

    requires java.sql;

    requires transitive com.clickhouse.client;
    // requires transitive com.google.gson;
    // requires transitive org.lz4.java;

    uses com.clickhouse.client.ClickHouseClient;
    uses com.clickhouse.client.ClickHouseDnsResolver;
    uses com.clickhouse.client.ClickHouseSslContextProvider;
    uses com.clickhouse.data.ClickHouseDataStreamFactory;
    uses com.clickhouse.logging.LoggerFactory;
}
