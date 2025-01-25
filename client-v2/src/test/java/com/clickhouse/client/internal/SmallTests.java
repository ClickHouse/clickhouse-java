package com.clickhouse.client.internal;

import com.clickhouse.client.api.data_formats.internal.ProcessParser;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class SmallTests {


    @Test
    public void testSummaryParser() {
        OperationMetrics operationMetrics = new OperationMetrics(null);
        String summary = createSummary(10, 200, 0, 0, 5, 6000);
        ProcessParser.parseSummary(summary, operationMetrics);

        Assert.assertEquals(operationMetrics.getMetric(ServerMetrics.NUM_ROWS_READ).getLong(), 10);
        Assert.assertEquals(operationMetrics.getMetric(ServerMetrics.NUM_BYTES_READ).getLong(), 200);

    }

    public static String createSummary(int readRows, int readBytes, int writtenRows,
                                        int writtenBytes, int totalRowsToRead, long elapsedNs) {
        return "{\"read_rows\":\"" + readRows + "\"," +
                "\"read_bytes\":\"" + readBytes + "\"," +
                "\"written_rows\":\"" + writtenRows + "\"," +
                "\"written_bytes\":\"" + writtenBytes + "\"," +
                "\"total_rows_to_read\":\"" + totalRowsToRead + "\"," +
                "\"elapsed_ns\":\"" + elapsedNs + "\"}";

    }

    @Test
    public void testTimezoneConvertion() {
        ZonedDateTime dt = ZonedDateTime.now();
        System.out.println(" now: " + dt);
        ZonedDateTime utcSameInstantDt = dt.withZoneSameInstant(ZoneId.of("UTC"));
        System.out.println("withZoneSameInstant: " + utcSameInstantDt);
        ZonedDateTime utcSameLocalDt = dt.withZoneSameLocal(ZoneId.of("UTC"));
        System.out.println("withZoneSameLocal: " + utcSameLocalDt);
    }
}
