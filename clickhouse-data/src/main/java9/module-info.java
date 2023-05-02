/**
 * Declares com.clickhouse.data module.
 */
module com.clickhouse.data {
    exports com.clickhouse.config;
    exports com.clickhouse.data;
    // exports com.clickhouse.data.cache;
    // exports com.clickhouse.data.format;
    // exports com.clickhouse.data.mapper;
    // exports com.clickhouse.data.stream;
    exports com.clickhouse.data.value;
    exports com.clickhouse.logging;

    requires static java.logging;
    requires static com.google.gson;
    requires static com.github.benmanes.caffeine;
    requires static org.lz4.java;
    requires static org.slf4j;
    requires static org.roaringbitmap;

    uses com.clickhouse.data.ClickHouseDataStreamFactory;
    uses com.clickhouse.logging.LoggerFactory;
}
