package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ProcessParser {

    private static final String[] SUMMARY_FIELDS = {
            "read_rows", "read_bytes", "written_rows", "written_bytes",
            "total_rows_to_read", "elapsed_ns", "result_rows"
    };

    private static final ServerMetrics[] SUMMARY_METRICS = {
            ServerMetrics.NUM_ROWS_READ, ServerMetrics.NUM_BYTES_READ,
            ServerMetrics.NUM_ROWS_WRITTEN, ServerMetrics.NUM_BYTES_WRITTEN,
            ServerMetrics.TOTAL_ROWS_TO_READ, ServerMetrics.ELAPSED_TIME,
            ServerMetrics.RESULT_ROWS
    };

    public static void parseSummary(String text, OperationMetrics metrics) {
        Map<String, Integer> map = parse(text == null ? "{}" : text);

        for (ServerMetrics m : ServerMetrics.values()) {
            metrics.updateMetric(m, -1);
        }

        for (int i = 0; i < SUMMARY_FIELDS.length; i++) {
            String field = SUMMARY_FIELDS[i];
            Integer value = map.get(field);
            if (value != null) {
                metrics.updateMetric(SUMMARY_METRICS[i], value);
            }
        }
    }


    public static Map<String, Integer> parse(String json) {
        if (json == null) {
            throw new IllegalArgumentException("json is null");
        }
        json = json.trim();
        if (json.isEmpty()) {
            return Collections.emptyMap();
        }
        if (json.charAt(0) != '{' || json.charAt(json.length() - 1) != '}') {
            throw new IllegalArgumentException("JSON must start with '{' and end with '}'");
        }

        Map<String, Integer> result = new HashMap<>();

        String content = json.substring(1, json.length() - 1).trim();
        if (content.isEmpty()) {
            return result; // empty object
        }

        String[] pairs = content.split(",");

        for (String pair : pairs) {
            String[] keyValue = pair.split(":", 2);
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid key-value format: " + pair);
            }

            String key = keyValue[0].trim();
            String valueStr = keyValue[1].trim();

            if (key.startsWith("\"") && key.endsWith("\"") && key.length() >= 2) {
                key = key.substring(1, key.length() - 1);
            } else {
                throw new IllegalArgumentException("Invalid key format: " + key);
            }

            if (valueStr.startsWith("\"") && valueStr.endsWith("\"") && valueStr.length() >= 2) {
                valueStr = valueStr.substring(1, valueStr.length() - 1);
            }

            try {
                int value = Integer.parseInt(valueStr);
                result.put(key, value);
            } catch (NumberFormatException e) {
                // ignore error
            }
        }

        return result;
    }

}
