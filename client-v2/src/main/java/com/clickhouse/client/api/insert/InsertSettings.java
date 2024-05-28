package com.clickhouse.client.api.insert;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InsertSettings {
    private String queryId;
    private int inputStreamBatchSize;
    private String operationId;
    Map<String, Object> rawSettings;

    public InsertSettings() {
        rawSettings = new HashMap<>();
        setDefaults();
    }

    public InsertSettings(Map<String, Object> settings) {
        rawSettings = new HashMap<>();
        setDefaults();
        rawSettings.putAll(settings);
    }

    private void setDefaults() {// Default settings, for now a very small list
        this.setInputStreamBatchSize(8196);
        this.queryId = null;
    }

    public Object getSetting(String option) {
        return rawSettings.get(option);
    }

    public void setSetting(String option, Object value) {
        rawSettings.put(option, value);
    }

    /**
     * Get all settings as an unmodifiable map.
     *
     * @return all settings
     */
    public Map<String, Object> getAllSettings() {
        return Collections.unmodifiableMap(rawSettings);
    }


    public InsertSettings setDeduplicationToken(String deduplicationToken) {
        rawSettings.put("insert_deduplication_token", deduplicationToken);
        return this;
    }


    public String getQueryId() {
        return this.queryId;
    }
    public InsertSettings setQueryId(String queryId) {
        this.queryId = queryId;
        return this;
    }


    public int getInputStreamBatchSize() {
        return this.inputStreamBatchSize;
    }
    public InsertSettings setInputStreamBatchSize(int inputStreamBatchSize) {
        this.inputStreamBatchSize = inputStreamBatchSize;
        return this;
    }

    public String getOperationId() {
        return this.operationId;
    }
    public InsertSettings setOperationId(String operationId) {
        this.operationId = operationId;
        return this;
    }
}
