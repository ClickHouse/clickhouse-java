package com.clickhouse.client.insert;

import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.hc.core5.http.HttpHeaders;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class InsertClientHttpCompressionTests extends InsertTests {

    public InsertClientHttpCompressionTests() {
        super(true, true);
    }


    @Test(groups = { "integration" }, dataProvider = "insertRawDataCompressedProvider")
    public void insertRawDataCompressed(String compressionAlgo) throws Exception {
        final String tableName = "raw_data_table";
        final String createSQL = "CREATE TABLE " + tableName +
                " (Id UInt32, event_ts Timestamp, name String, p1 Int64, p2 String) ENGINE = MergeTree() ORDER BY ()";

        initTable(tableName, createSQL);

        InsertSettings insertSettings = InsertSettings.merge(settings, new InsertSettings());
        insertSettings.setInputStreamCopyBufferSize(8198 * 2);
        insertSettings.httpHeader(HttpHeaders.CONTENT_ENCODING, compressionAlgo);
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(data);
        for (int i = 0; i < 1000; i++) {
            writer.printf("%d\t%s\t%s\t%d\t%s\n", i, "2021-01-01 00:00:00", "name" + i, i, "p2");
        }
        writer.flush();
        InsertResponse response = client.insert(tableName, new ByteArrayInputStream(data.toByteArray()),
                ClickHouseFormat.TSV, insertSettings).get(30, TimeUnit.SECONDS);
        OperationMetrics metrics = response.getMetrics();
        assertEquals((int)response.getWrittenRows(), 1000 );

        List<GenericRecord> records = client.queryAll("SELECT * FROM " + tableName);
        assertEquals(records.size(), 1000);

        for (int i = 0; i < records.size(); i++) {
            assertEquals(records.get(i).getInteger(1), i);
            assertEquals(records.get(i).getString("event_ts"), "2021-01-01 00:00:00");
            assertEquals(records.get(i).getString("name"), "name" + i);
            assertEquals(records.get(i).getInteger("p1"), i);
            assertEquals(records.get(i).getString("p2"), "p2");
        }
    }

    @DataProvider(name = "insertRawDataCompressedProvider")
    public Object[][] insertRawDataCompressedProvider() {
        return new Object[][] {
            { "lz4" },
            { "zstd" },
            { "deflate" },
            { "gz" },
        };
    }
}
