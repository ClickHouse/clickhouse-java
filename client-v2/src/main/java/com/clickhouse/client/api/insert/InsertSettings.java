package com.clickhouse.client.api.insert;

import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;

import java.util.HashMap;
import java.util.Map;

public class InsertSettings {
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
    }

    public Object getSetting(String option) {
        return rawSettings.get(option);
    }

    public void setSetting(String option, Object value) {
        rawSettings.put(option, value);
    }


    public ClickHouseFormat getFormat() {
        return (ClickHouseFormat) rawSettings.get(ClickHouseClientOption.FORMAT.getKey());
    }
    public InsertSettings setFormat(ClickHouseFormat format) {
        rawSettings.put(ClickHouseClientOption.FORMAT.getKey(), format);
        return this;
    }


    public String getDeduplicationToken() {
        return (String) rawSettings.get("insert_deduplication_token");
    }
    public InsertSettings setDeduplicationToken(String deduplicationToken) {
        rawSettings.put("insert_deduplication_token", deduplicationToken);
        return this;
    }


    public String getQueryId() {
        return (String) rawSettings.get("query_id");
    }
    public InsertSettings setQueryId(String queryId) {
        rawSettings.put("query_id", queryId);
        return this;
    }


    public int getInputStreamBatchSize() {
        return (int) rawSettings.get("input_stream_batch_size");
    }
    public InsertSettings setInputStreamBatchSize(int inputStreamBatchSize) {
        rawSettings.put("input_stream_batch_size", inputStreamBatchSize);
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
