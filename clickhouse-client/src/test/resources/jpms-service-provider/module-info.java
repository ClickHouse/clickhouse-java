module test.clickhouse.client.provider {
    requires com.clickhouse.client;

    provides com.clickhouse.client.ClickHouseDnsResolver with test.provider.TestDnsResolver;
    provides com.clickhouse.client.ClickHouseRequestManager with test.provider.TestRequestManager;
}
