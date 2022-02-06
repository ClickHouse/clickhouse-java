package com.clickhouse.client.grpc;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClientIntegrationTest;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.ClickHouseFormat;

public class ClickHouseGrpcClientTest extends ClientIntegrationTest {
    @Override
    protected ClickHouseProtocol getProtocol() {
        return ClickHouseProtocol.GRPC;
    }

    @Override
    protected Class<? extends ClickHouseClient> getClientClass() {
        return ClickHouseGrpcClient.class;
    }

    @Test(groups = "integration")
    public void testResponseSummary() throws Exception {
        ClickHouseNode server = getServer();

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = client.connect(server)
                        .option(ClickHouseClientOption.MAX_BUFFER_SIZE, 8)
                        .format(ClickHouseFormat.TabSeparatedWithNamesAndTypes)
                        .query("select number, number+1 from numbers(100)").execute().get()) {
            int n = 0;
            for (ClickHouseRecord record : resp.records()) {
                Assert.assertEquals(record.size(), 2);
                Assert.assertEquals(record.getValue(0).asInteger(), n++);
                Assert.assertEquals(record.getValue(1).asInteger(), n);
            }
            Assert.assertEquals(n, 100);

            ClickHouseResponseSummary summary = resp.getSummary();
            Assert.assertEquals(summary.getReadRows(), n);
            Assert.assertEquals(summary.getStatistics().getRows(), n);
        }
    }
}
