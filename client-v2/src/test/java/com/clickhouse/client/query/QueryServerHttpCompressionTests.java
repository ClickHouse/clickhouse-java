package com.clickhouse.client.query;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.enums.CompressionAlgorithm;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QuerySettings;
import org.apache.hc.core5.http.HttpHeaders;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryServerHttpCompressionTests extends QueryTests {
    QueryServerHttpCompressionTests() {
        super(true, true);
    }


    @Test(groups = {"integration"}, dataProvider = "testQueryCompressedProvider")
    public void testQueryCompressed(String compressAlgo) throws Exception {
        List<Map<String, Object>> dataset = prepareDataSet(DATASET_TABLE, DATASET_COLUMNS, DATASET_VALUE_GENERATORS, 10);
        QuerySettings settings = new QuerySettings();
        settings.httpHeader(HttpHeaders.ACCEPT_ENCODING, compressAlgo);
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + DATASET_TABLE + " LIMIT " + dataset.size(), settings);
        Assert.assertFalse(records.isEmpty());

        for (String colDefinition : DATASET_COLUMNS) {
            // result values
            String colName = colDefinition.split(" ")[0];
            List<Object> colValues = records.stream().map(r -> {
                        Object v = r.getObject(colName);
                        if (v instanceof BinaryStreamReader.ArrayValue) {
                            v = ((BinaryStreamReader.ArrayValue)v).asList();
                        }

                        return v;
                    }

            ).collect(Collectors.toList());
            Assert.assertEquals(colValues.size(), dataset.size());

            // dataset values
            List<Object> dataValue = dataset.stream().map(d -> d.get(colName)).collect(Collectors.toList());
            Assert.assertEquals(colValues, dataValue, "Failed for column " + colName);
        }
    }

    @Test(groups = {"integration"}, dataProvider = "compressionAlgorithms")
    public void testQueryWithCompressionAlgorithmAndHttpCompression(CompressionAlgorithm algorithm) throws Exception {
        try (Client client = newClient()
                .compressionAlgorithm(algorithm)
                .compressClientRequest(true)
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

    @DataProvider(name = "testQueryCompressedProvider")
    public Object[][] testQueryCompressedProvider() {
        return new Object[][] {
                { "lz4" },
                { "zstd" },
                { "deflate" },
                { "gzip" },
        };
    }
}
