/**
 * Declares com.clickhouse module.
 */
module com.clickhouse.jdbc {
    exports com.clickhouse.jdbc;
    
    requires java.sql;

    requires transitive com.clickhouse.client;
    requires transitive com.google.gson;
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
