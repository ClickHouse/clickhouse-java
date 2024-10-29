package com.clickhouse.client.api.insert;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientSettings;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.config.ClickHouseClientOption;

import java.util.Collection;
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

    /**
     * Defines list of headers that should be sent with current request. The Client will use a header value
     * defined in {@code headers} instead of any other.
     *
     * @see Client.Builder#httpHeaders(Map)
     * @param key - header name.
     * @param value - header value.
     * @return same instance of the builder
     */
    public InsertSettings httpHeader(String key, String value) {
        rawSettings.put(ClientSettings.HTTP_HEADER_PREFIX + key, value);
        return this;
    }

    /**
     * {@see #httpHeader(String, String)} but for multiple values.
     * @param key - name of the header
     * @param values - collection of values
     * @return same instance of the builder
     */
    public InsertSettings httpHeader(String key, Collection<String> values) {
        rawSettings.put(ClientSettings.HTTP_HEADER_PREFIX + key, ClientSettings.commaSeparated(values));
        return this;
    }

    /**
     * {@see #httpHeader(String, String)} but for multiple headers.
     * @param headers - map of headers
     * @return same instance of the builder
     */
    public InsertSettings httpHeaders(Map<String, String> headers) {
        headers.forEach(this::httpHeader);
        return this;
    }

    /**
     * Defines list of server settings that should be sent with each request. The Client will use a setting value
     * defined in {@code settings} instead of any other.
     * Operation settings may override these values.
     *
     * @see Client.Builder#serverSetting(String, Collection)
     * @param name - name of the setting
     * @param value - value of the setting
     * @return same instance of the builder
     */
    public InsertSettings serverSetting(String name, String value) {
        rawSettings.put(ClientSettings.SERVER_SETTING_PREFIX + name, value);
        return this;
    }

    /**
     * {@see #serverSetting(String, String)} but for multiple values.
     * @param name - name of the setting without special prefix
     * @param values - collection of values
     * @return same instance of the builder
     */
    public InsertSettings serverSetting(String name, Collection<String> values) {
        rawSettings.put(ClientSettings.SERVER_SETTING_PREFIX + name, ClientSettings.commaSeparated(values));
        return this;
    }

    /**
     * Sets DB roles for an operation. Roles that were set by {@link Client#setDBRoles(Collection)} will be overridden.
     *
     * @param dbRoles
     */
    public InsertSettings setDBRoles(Collection<String> dbRoles) {
        rawSettings.put(ClientSettings.SESSION_DB_ROLES, dbRoles);
        return this;
    }

    /**
     * Gets DB roles for an operation.
     *
     * @return list of DB roles
     */
    public Collection<String> getDBRoles() {
        return (Collection<String>) rawSettings.get(ClientSettings.SESSION_DB_ROLES);
    }

    /**
     * Sets the comment that will be added to the query log record associated with the query.
     * @param logComment - comment to be added to the log
     * @return same instance of the builder
     */
    public InsertSettings logComment(String logComment) {
        this.logComment = logComment;
        if (logComment != null && !logComment.isEmpty()) {
            rawSettings.put(ClientSettings.SETTING_LOG_COMMENT, logComment);
        }
        return this;
    }

    private String logComment = null;

    public String getLogComment() {
        return logComment;
    }
}
