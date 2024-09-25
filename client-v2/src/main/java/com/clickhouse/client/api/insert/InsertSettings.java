package com.clickhouse.client.api.insert;

import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.client.config.ClickHouseClientOption;

import java.util.HashMap;
import java.util.Map;

public class InsertSettings {
    private static final int DEFAULT_INPUT_STREAM_BATCH_SIZE = 8196;

    private int inputStreamCopyBufferSize;
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
        this.setInputStreamCopyBufferSize(DEFAULT_INPUT_STREAM_BATCH_SIZE);
    }

    /**
     * Gets a configuration option.
     *
     * @param option - configuration option name
     * @return configuration option value
     */
    public Object getOption(String option) {
        return rawSettings.get(option);
    }

    /**
     * Sets a configuration option. This method can be used to set any configuration option.
     * There is no specific validation is done on the key or value.
     *
     * @param option - configuration option name
     * @param value  - configuration option value
     */
    public void setOption(String option, Object value) {
        rawSettings.put(option, value);
    }

    /**
     * Get all settings as an unmodifiable map.
     *
     * @return all settings
     */
    public Map<String, Object> getAllSettings() {
        return rawSettings;
    }

    /**
     * Sets the deduplication token. This token will be sent to the server and can be used to identify the query.
     *
     * @param token - deduplication token
     * @return
     */
    public InsertSettings setDeduplicationToken(String token) {
        rawSettings.put("insert_deduplication_token", token);
        return this;
    }

    public String getQueryId() {
        return (String) rawSettings.get(ClickHouseClientOption.QUERY_ID.getKey());
    }

    /**
     * Sets the query id. This id will be sent to the server and can be used to identify the query.
     */
    public InsertSettings setQueryId(String queryId) {
        rawSettings.put(ClickHouseClientOption.QUERY_ID.getKey(), queryId);
        return this;
    }

    public int getInputStreamCopyBufferSize() {
        return this.inputStreamCopyBufferSize;
    }

    /**
     * Copy buffer size. The buffer is used while write operation to copy data from user provided input stream
     * to an output stream.
     */
    public InsertSettings setInputStreamCopyBufferSize(int size) {
        this.inputStreamCopyBufferSize = size;
        return this;
    }

    /**
     * Operation id. Used internally to register new operation.
     * Should not be called directly.
     */
    public String getOperationId() {
        return this.operationId;
    }

    /**
     * Operation id. Used internally to register new operation.
     * Should not be called directly.
     *
     * @param operationId - operation id
     */
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
     * Client request compression. If set to true client will compress the request.
     *
     * @param enabled - indicates if client request compression is enabled
     */
    public InsertSettings compressClientRequest(boolean enabled) {
        this.rawSettings.put("decompress", enabled);
        return this;
    }

    public boolean isClientRequestEnabled() {
        return (Boolean) rawSettings.get("decompress");
    }
}
