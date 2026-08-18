package test.provider;

import com.clickhouse.client.ClickHouseDnsResolver;
import com.clickhouse.client.ClickHouseRequestManager;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) {
        if (ClickHouseDnsResolver.getInstance().getClass() != TestDnsResolver.class) {
            throw new AssertionError("ClickHouseDnsResolver provider was not loaded");
        }
        if (ClickHouseRequestManager.getInstance().getClass() != TestRequestManager.class) {
            throw new AssertionError("ClickHouseRequestManager provider was not loaded");
        }
    }
}
