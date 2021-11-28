package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseProtocolTest {
    @Test(groups = { "unit" })
    public void testUriScheme() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> ClickHouseProtocol.GRPC.getUriSchemes().add("a"));
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> ClickHouseProtocol.HTTP.getUriSchemes().remove(0));

        for (ClickHouseProtocol p : ClickHouseProtocol.values()) {
            for (String s : p.getUriSchemes()) {
                Assert.assertEquals(ClickHouseProtocol.fromUriScheme(s), p);
                Assert.assertEquals(ClickHouseProtocol.fromUriScheme(s.toUpperCase()), p);
                Assert.assertEquals(ClickHouseProtocol.fromUriScheme(s + " "), ClickHouseProtocol.ANY);
            }
        }

        Assert.assertEquals(ClickHouseProtocol.fromUriScheme("gRPC"), ClickHouseProtocol.GRPC);
    }
}
