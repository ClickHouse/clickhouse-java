package com.clickhouse.client.query;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.CompressionAlgorithm;
import com.clickhouse.client.api.query.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

public class QueryServerContentCompressionTests extends QueryTests {

    QueryServerContentCompressionTests() {
        super(true, false);
    }

    @Test(groups = {"integration"}, dataProvider = "compressionAlgorithms")
    public void testQueryWithCompressionAlgorithm(CompressionAlgorithm algorithm) throws Exception {
        try (Client client = newClient().compressionAlgorithm(algorithm).build()) {
            List<GenericRecord> records = client.queryAll("SELECT number, toString(number) AS str " +
                    "FROM system.numbers LIMIT 1000");

            Assert.assertEquals(records.size(), 1000);
            Assert.assertEquals(records.get(0).getLong("number"), 0);
            Assert.assertEquals(records.get(999).getLong("number"), 999);
            Assert.assertEquals(records.get(999).getString("str"), "999");
        }
    }

    @Test(groups = {"integration"})
    public void testQueryWithoutCompression() throws Exception {
        // the algorithm selects only how a body is compressed - the flags select whether it is
        try (Client client = newClient()
                .compressionAlgorithm(CompressionAlgorithm.ZSTD)
                .compressServerResponse(false)
                .compressClientRequest(false)
                .build()) {
            List<GenericRecord> records = client.queryAll("SELECT number, toString(number) AS str " +
                    "FROM system.numbers LIMIT 1000");

            Assert.assertEquals(records.size(), 1000);
            Assert.assertEquals(records.get(999).getLong("number"), 999);
            Assert.assertEquals(records.get(999).getString("str"), "999");
        }
    }

    @DataProvider(name = "compressionAlgorithms")
    public Object[][] compressionAlgorithms() {
        return new Object[][]{
                {CompressionAlgorithm.LZ4},
                {CompressionAlgorithm.ZSTD},
        };
    }
}
