package com.clickhouse.client.api;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;

import java.util.HashMap;
import java.util.Map;

public class InsertSettings {
    Map<String, Object> rawSettings;

    public InsertSettings() {
        rawSettings = new HashMap<>();
    }

    public InsertSettings(Map<String, Object> settings) {
        rawSettings = new HashMap<>();
        rawSettings.putAll(settings);
    }

    public Object getSetting(String option) {
        return rawSettings.get(option);
    }

    public void setSetting(String option, Object value) {
        rawSettings.put(option, value);
    }


    public InsertSettings setFormat(ClickHouseFormat format) {
        rawSettings.put(ClickHouseClientOption.FORMAT.getKey(), format);
        return this;
    }

    public InsertSettings setDeduplicationToken(String deduplicationToken) {
        rawSettings.put("insert_deduplication_token", deduplicationToken);
        return this;
    }

    public InsertSettings setQueryId(String queryId) {
        rawSettings.put("query_id", queryId);
        return this;
    }
}
