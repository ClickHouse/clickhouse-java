package com.clickhouse.client.grpc;

import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClientIntegrationTest;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseInputStream;

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
                        .option(ClickHouseClientOption.READ_BUFFER_SIZE, 8)
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

    @Test(groups = { "unit" })
    public void testLZ4FrameStream() throws IOException {
        // python-lz4
        // lz4.frame.decompress(b'\x04\x22\x4D\x18\x40\x50\x77\x3B\x00\x00\x00\xFF\x21\x5C\x27\x37\x33\x36\x65\x63\x32\x61\x36\x2D\x65\x39\x39\x38\x2D\x34\x62\x63\x62\x2D\x39\x32\x66\x38\x2D\x62\x30\x37\x31\x31\x35\x61\x63\x66\x34\x62\x33\x5C\x27\x0A\x53\x74\x72\x69\x6E\x67\x0A\x2E\x00\x0D\x50\x66\x34\x62\x33\x0A\x00\x00\x00\x00')
        // b"\\'736ec2a6-e998-4bcb-92f8-b07115acf4b3\\'\nString\n736ec2a6-e998-4bcb-92f8-b07115acf4b3\n"
        byte[] bytes = new byte[] { 4, 34, 77, 24, 64, 80, 119, 59, 0, 0, 0, -1, 33, 92, 39, 55, 51, 54, 101, 99, 50,
                97, 54, 45, 101, 57, 57, 56, 45, 52, 98, 99, 98, 45, 57, 50, 102, 56, 45, 98, 48, 55, 49, 49, 53, 97,
                99, 102, 52, 98, 51, 92, 39, 10, 83, 116, 114, 105, 110, 103, 10, 46, 0, 13, 80, 102, 52, 98, 51, 10,
                0, 0, 0, 0 };

        String expected = "\\'736ec2a6-e998-4bcb-92f8-b07115acf4b3\\'\nString\n736ec2a6-e998-4bcb-92f8-b07115acf4b3\n";
        Assert.assertEquals(
                ClickHouseInputStream.of(new FramedLZ4CompressorInputStream(new ByteArrayInputStream(bytes), false))
                        .readCustom((b, o, l) -> -1).asAsciiString(),
                expected);

    }
}
