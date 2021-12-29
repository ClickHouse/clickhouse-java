/**
 * Declares ru.yandex.clickhouse module.
 */
module com.clickhouse.jdbc {
    exports com.clickhouse.jdbc;
    
    exports ru.yandex.clickhouse;
    exports ru.yandex.clickhouse.domain;
    exports ru.yandex.clickhouse.except;
    exports ru.yandex.clickhouse.response;
    exports ru.yandex.clickhouse.settings;
    exports ru.yandex.clickhouse.util;

    requires transitive com.clickhouse.client;
    requires transitive com.google.gson;
    requires transitive org.apache.httpcomponents.httpclient;
    requires transitive org.apache.httpcomponents.httpmime;
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
