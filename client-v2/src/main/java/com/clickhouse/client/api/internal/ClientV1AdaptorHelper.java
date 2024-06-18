package com.clickhouse.client.api.internal;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseClientBuilder;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseProxyType;
import com.clickhouse.config.ClickHouseOption;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ClientV1AdaptorHelper {

    private static void copyProxySettings(Map<ClickHouseOption, Serializable> target, Map<String, String> config) {
        ClickHouseClientOption opt = ClickHouseClientOption.PROXY_HOST;
        String value = config.get(opt.getKey());
        if (value != null) {
            target.put(opt, value);
        }
        opt = ClickHouseClientOption.PROXY_PORT;
        value = config.get(opt.getKey());
        if (value != null) {
            target.put(opt, Integer.parseInt(value));
        }
        opt = ClickHouseClientOption.PROXY_TYPE;
        value = config.get(opt.getKey());
        if (value != null) {
            target.put(opt, ClickHouseProxyType.valueOf(value));
        }
    }

    public static ClickHouseClient createClient(Map<String, String> configuration) {
        Map<ClickHouseOption, Serializable> config = new HashMap<>();
        copyProxySettings(config, configuration);

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
}
