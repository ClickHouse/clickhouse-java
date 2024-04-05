package com.clickhouse.client.api.query;


import com.clickhouse.client.config.ClickHouseClientOption;

import java.util.HashMap;
import java.util.Map;

public class QuerySettings {

    private Map<String, Object> rawSettings;

    public QuerySettings() {
        this.rawSettings = new HashMap<>();
    }

    public QuerySettings setSetting(String key, Object value) {
        rawSettings.put(key, value);
        return this;
    }
    public Object getSetting(String key) {
        return rawSettings.get(key);
    }
    public QuerySettings appendToSetting(String key, Object value) {
        rawSettings.put(key, value);
        return this;
    }

    public QuerySettings setFormat(String format) {
        rawSettings.put(ClickHouseClientOption.FORMAT.getKey(), format);
        return this;
    }

    public String getFormat() {
        return (String) rawSettings.get(ClickHouseClientOption.FORMAT.getKey());
    }

    public QuerySettings setQueryID(String queryID) {
        rawSettings.put("query_id", queryID);
        return this;
    }
    public String getQueryID() {
        return (String) rawSettings.get("query_id");
    }
}
