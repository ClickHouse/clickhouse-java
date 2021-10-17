/**
 * Declares com.clickhouse.client module.
 */
module com.clickhouse.client {
    exports com.clickhouse.client;
    exports com.clickhouse.client.config;
    exports com.clickhouse.client.data;
    exports com.clickhouse.client.exception;
    
    exports com.clickhouse.client.logging to
        com.clickhouse.client.http,
        com.clickhouse.client.grpc,
        com.clickhouse.client.mysql,
        com.clickhouse.client.postgresql,
        // native is a reserved keyword :<
        com.clickhouse.client.tcp,
        com.clickhouse.jdbc,
        ru.yandex.clickhouse;

    requires static java.logging;
    requires static com.google.gson;
    requires static com.github.benmanes.caffeine;
    requires static org.dnsjava;
    requires static org.lz4.java;
    requires static org.slf4j;
    requires static org.roaringbitmap;

    uses com.clickhouse.client.ClickHouseClient;
    uses com.clickhouse.client.ClickHouseDataStreamFactory;
    uses com.clickhouse.client.ClickHouseDnsResolver;
    uses com.clickhouse.client.ClickHouseSslContextProvider;
    uses com.clickhouse.client.logging.LoggerFactory;
}
