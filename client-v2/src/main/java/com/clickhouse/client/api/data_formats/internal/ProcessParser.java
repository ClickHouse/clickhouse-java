package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

public class ProcessParser {

    private static JsonFactory parserFactory = new JsonFactory();

    public static void parseSummary(String text, OperationMetrics metrics) {
        try (JsonParser parser = parserFactory.createParser(text)) {
            parser.nextToken(); // skip START_OBJECT
            JsonToken t = parser.nextToken();

            for (ServerMetrics m : ServerMetrics.values()) {
                metrics.updateMetric(m, -1);
            }
            while (t != null) {
                if (t == JsonToken.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextValue();
                    if ("read_rows".equals(fieldName)) {
                        metrics.updateMetric(ServerMetrics.NUM_ROWS_READ, parser.getValueAsLong());
                    } else if ("read_bytes".equals(fieldName)) {
                        metrics.updateMetric(ServerMetrics.NUM_BYTES_READ, parser.getValueAsLong());
                    } else if ("written_rows".equals(fieldName)) {
                        metrics.updateMetric(ServerMetrics.NUM_ROWS_WRITTEN, parser.getValueAsLong());
                    } else if ("written_bytes".equals(fieldName)) {
                        metrics.updateMetric(ServerMetrics.NUM_BYTES_WRITTEN, parser.getValueAsLong());
                    } else if ("total_rows_to_read".equals(fieldName)) {
                        metrics.updateMetric(ServerMetrics.TOTAL_ROWS_TO_READ, parser.getValueAsLong());
                    } else if ("elapsed_ns".equals(fieldName)) {
                        metrics.updateMetric(ServerMetrics.ELAPSED_TIME, parser.getValueAsLong());
                    } else if ("result_rows".equals(fieldName)) {
                        metrics.updateMetric(ServerMetrics.RESULT_ROWS, parser.getValueAsLong());
                    } else {
                        // ignore unknown fields for forward compatibility
                    }
                }
                t = parser.nextToken();
            }
        } catch (IOException e) {
            throw new ClientException("Failed to parse summary", e);
        }
    }
}
