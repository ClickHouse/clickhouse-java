package com.clickhouse.client.api.internal;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseClientBuilder;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.config.ClickHouseOption;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ClientV1AdaptorHelper {

    private static void copyClientOptions(Map<ClickHouseOption, Serializable> target, Map<String, String> config) {

        for (ClickHouseClientOption opt : ClickHouseClientOption.values()) {
            String value = config.get(opt.getKey());
            if (value == null) {
                continue;
            }

            if (opt.getValueType().isAssignableFrom(Integer.class)) {
                target.put(opt, Integer.parseInt(value));
            } else if (opt.getValueType().isAssignableFrom(Boolean.class)) {
                target.put(opt, Boolean.parseBoolean(value));
            } else if (opt.getValueType().isEnum()) {
                target.put(opt, Enum.valueOf((Class<Enum>) opt.getValueType(), value));
            } else if (opt.getValueType().isAssignableFrom(String.class)) {
                target.put(opt, value);
            }
        }
    }

    public static ClickHouseClient createClient(Map<String, String> configuration) {
        Map<ClickHouseOption, Serializable> config = new HashMap<>();
        copyClientOptions(config, configuration);

        ClickHouseConfig clientConfig = new ClickHouseConfig(config);

        ClickHouseClientBuilder clientV1 = ClickHouseClient.builder()
                .config(clientConfig)
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP));
        return clientV1.build();
    }

    public static ClickHouseRequest.Mutation createMutationRequest(ClickHouseRequest.Mutation request,
                                                            String tableName,
                                                            InsertSettings settings,
                                                            Map<String, String> configuration) {
        if (settings.getQueryId() != null) {//This has to be handled separately
            request.table(tableName, settings.getQueryId());
        } else {
            request.table(tableName);
        }

        Map<String, Object> opSettings = settings == null ? Collections.emptyMap() : settings.getAllSettings();
        //For each setting, set the value in the request
        for (Map.Entry<String, Object> entry : opSettings.entrySet()) {
            request.set(entry.getKey(), String.valueOf(entry.getValue()));
        }

        return request;
    }

    public static void setServerStats(ClickHouseResponseSummary.Progress progress, OperationMetrics metrics) {
        metrics.updateMetric(ServerMetrics.NUM_ROWS_READ, progress.getReadRows());
        metrics.updateMetric(ServerMetrics.NUM_ROWS_WRITTEN, progress.getWrittenRows());
        metrics.updateMetric(ServerMetrics.TOTAL_ROWS_TO_READ, progress.getTotalRowsToRead());
        metrics.updateMetric(ServerMetrics.NUM_BYTES_READ, progress.getReadBytes());
        metrics.updateMetric(ServerMetrics.NUM_BYTES_WRITTEN, progress.getWrittenBytes());
        metrics.updateMetric(ServerMetrics.RESULT_ROWS, progress.getResultRows());
        metrics.updateMetric(ServerMetrics.ELAPSED_TIME, progress.getElapsedTime());
    }
}
