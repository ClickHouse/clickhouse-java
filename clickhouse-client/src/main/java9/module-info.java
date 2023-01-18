/**
 * Declares com.clickhouse.client module.
 */
module com.clickhouse.client {
    exports com.clickhouse.client;
    exports com.clickhouse.client.config;

    requires static java.logging;
    requires static org.slf4j;

    requires transitive com.clickhouse.data;

    uses com.clickhouse.client.ClickHouseClient;
    uses com.clickhouse.client.ClickHouseDnsResolver;
    uses com.clickhouse.client.ClickHouseSslContextProvider;
}
