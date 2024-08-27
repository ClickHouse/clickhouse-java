/**
 * Declares com.clickhouse.r2dbc module.
 */
module com.clickhouse.r2dbc {
    exports com.clickhouse.r2dbc;

    requires transitive com.clickhouse.client;
    requires transitive r2dbc.spi;
    requires transitive reactor.core;
    requires transitive org.lz4.java;

    uses com.clickhouse.client.ClickHouseClient;
    uses com.clickhouse.client.ClickHouseDnsResolver;
    uses com.clickhouse.client.ClickHouseSslContextProvider;
    uses com.clickhouse.data.ClickHouseDataStreamFactory;
    uses com.clickhouse.logging.LoggerFactory;
}
