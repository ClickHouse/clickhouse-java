/**
 * Declares ru.yandex.clickhouse module.
 */
module com.clickhouse.r2dbc {
    exports com.clickhouse.r2dbc;

    requires transitive com.clickhouse.client;
    requires transitive r2dbc.spi;
    requires transitive reactor.core;
    requires transitive org.lz4.java;

    requires static java.logging;
    // requires static com.github.benmanes.caffeine;
    // requires static org.dnsjava;
    // requires static org.slf4j;
    requires static org.roaringbitmap;

    uses com.clickhouse.client.ClickHouseClient;
    uses com.clickhouse.client.ClickHouseDataStreamFactory;
    uses com.clickhouse.client.ClickHouseDnsResolver;
    uses com.clickhouse.client.ClickHouseSslContextProvider;
    uses com.clickhouse.client.logging.LoggerFactory;
}
