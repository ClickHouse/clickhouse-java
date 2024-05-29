package com.clickhouse.client.api.insert;

import com.clickhouse.client.api.internal.ValidationUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InsertSettings {
    private static final int DEFAULT_INPUT_STREAM_BATCH_SIZE = 8196;

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
        this.setInputStreamBatchSize(DEFAULT_INPUT_STREAM_BATCH_SIZE);
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

    /**
     * Sets database to be used for a request.
     */
    public InsertSettings setDatabase(String database) {
        ValidationUtils.checkNonBlank(database, "database");
        rawSettings.put("database", database);
        return this;
    }

    public String getDatabase() {
        return (String) rawSettings.get("database");
    }

    /**
     * Whether the client will compress response it sends to client.
     */
    public InsertSettings enableCompression(Boolean compress) {
        ValidationUtils.checkNotNull(compress, "compress");
        rawSettings.put("compress", compress);
        return this;
    }

    public Boolean isCompressionEnabled() {
        return (Boolean) rawSettings.get("compress");
    }
}
