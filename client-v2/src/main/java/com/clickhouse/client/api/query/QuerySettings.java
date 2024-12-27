package com.clickhouse.client.api.query;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.data.ClickHouseFormat;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * <p>Query settings class represents a set of settings that can be used to customize query execution.</p>
 *
 */
public class QuerySettings {

    public static final int MINIMAL_READ_BUFFER_SIZE = 8192;

    private Map<String, Object> rawSettings;

    public QuerySettings() {
        this.rawSettings = new HashMap<>();
    }

    /**
     * Sets a configuration option. This method can be used to set any configuration option.
     * There is no specific validation is done on the key or value.
     *
     * @param option - configuration option name
     * @param value - configuration option value
     */
    public QuerySettings setOption(String option, Object value) {
        rawSettings.put(option, value);
        if (option.equals(ClientConfigProperties.PRODUCT_NAME.getKey())) {
            rawSettings.put(ClientConfigProperties.CLIENT_NAME.getKey(), value);
        }

        return this;
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
     * Get raw settings. Returns reference to internal map, so any changes will affect this object.
     *
     * @return all settings map
     */
    public Map<String, Object> getAllSettings() {
        return rawSettings;
    }

    /**
     * Sets the query id. This id will be sent to the server and can be used to identify the query.
     */
    public QuerySettings setQueryId(String queryId) {
        rawSettings.put("query_id", queryId);
        return this;
    }

    public String getQueryId() {
        return (String) rawSettings.get("query_id");
    }

    /**
     * Read buffer is used for reading data from a server. Size is in bytes.
     * Minimal value is {@value MINIMAL_READ_BUFFER_SIZE} bytes.
     */
    public QuerySettings setReadBufferSize(Integer size) {
        ValidationUtils.checkNotNull(size, "read_buffer_size");
        ValidationUtils.checkRange(size, MINIMAL_READ_BUFFER_SIZE, Integer.MAX_VALUE, "read_buffer_size");
        rawSettings.put("read_buffer_size", size);
        return this;
    }

    public Integer getReadBufferSize() {
        return (Integer) rawSettings.get("read_buffer_size");
    }

    /**
     * Sets output format for a server response.
     */
    public QuerySettings setFormat(ClickHouseFormat format) {
        rawSettings.put("format", format);
        return this;
    }

    public ClickHouseFormat getFormat() {
        return (ClickHouseFormat) rawSettings.get("format");
    }

    /**
     * Maximum query execution time in seconds on server. 0 means no limit.
     * If query is not finished in this time then server will send an exception.
     */
    public QuerySettings setMaxExecutionTime(Integer maxExecutionTime) {
        rawSettings.put("max_execution_time", maxExecutionTime);
        return this;
    }

    public Integer getMaxExecutionTime() {
        return (Integer) rawSettings.get("max_execution_time");
    }

    /**
     * Sets database to be used for a request.
     */
    public QuerySettings setDatabase(String database) {
        ValidationUtils.checkNonBlank(database, "database");
        rawSettings.put("database", database);
        return this;
    }

    public String getDatabase() {
        return (String) rawSettings.get("database");
    }

    /**
     * Requests the server to wait for the and of the query before sending response. Useful for getting accurate summary.
     */
    public QuerySettings waitEndOfQuery(Boolean waitEndOfQuery) {
        serverSetting(ServerSettings.WAIT_END_OF_QUERY,  waitEndOfQuery ? "1" : "0");
        return this;
    }

    public QuerySettings setUseServerTimeZone(Boolean useServerTimeZone) {
        if (rawSettings.containsKey(ClientConfigProperties.USE_TIMEZONE.getKey())) {
            throw new ValidationUtils.SettingsValidationException(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey(),
                    "Cannot set both use_time_zone and use_server_time_zone");
        }
        rawSettings.put(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey(), useServerTimeZone);
        return this;
    }

    public Boolean getUseServerTimeZone() {
        return (Boolean) rawSettings.get(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey());
    }

    public QuerySettings setUseTimeZone(String timeZone) {
        if (rawSettings.containsKey(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey())) {
            throw new ValidationUtils.SettingsValidationException(ClientConfigProperties.USE_TIMEZONE.getKey(),
                    "Cannot set both use_time_zone and use_server_time_zone");
        }
        rawSettings.put(ClientConfigProperties.USE_TIMEZONE.getKey(), TimeZone.getTimeZone(timeZone));
        return this;
    }

    public TimeZone getServerTimeZone() {
        return (TimeZone) rawSettings.get(ClientConfigProperties.SERVER_TIMEZONE.getKey());
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
    public QuerySettings httpHeader(String key, String value) {
        rawSettings.put(ClientConfigProperties.HTTP_HEADER_PREFIX + key, value);
        return this;
    }

    /**
     * {@see #httpHeader(String, String)} but for multiple values.
     * @param key - name of the header
     * @param values - collection of values
     * @return same instance of the builder
     */
    public QuerySettings httpHeader(String key, Collection<String> values) {
        rawSettings.put(ClientConfigProperties.HTTP_HEADER_PREFIX + key, ClientConfigProperties.commaSeparated(values));
        return this;
    }

    /**
     * {@see #httpHeader(String, String)} but for multiple headers.
     * @param headers - map of headers
     * @return same instance of the builder
     */
    public QuerySettings httpHeaders(Map<String, String> headers) {
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
    public QuerySettings serverSetting(String name, String value) {
        rawSettings.put(ClientConfigProperties.serverSetting(name), value);
        return this;
    }

    /**
     * {@see #serverSetting(String, String)} but for multiple values.
     * @param name - name of the setting without special prefix
     * @param values - collection of values
     * @return same instance of the builder
     */
    public QuerySettings serverSetting(String name, Collection<String> values) {
        rawSettings.put(ClientConfigProperties.serverSetting(name), ClientConfigProperties.commaSeparated(values));
        return this;
    }

    /**
     * Sets DB roles for an operation. Roles that were set by {@link Client#setDBRoles(Collection)} will be overridden.
     *
     * @param dbRoles
     */
    public QuerySettings setDBRoles(Collection<String> dbRoles) {
        rawSettings.put(ClientConfigProperties.SESSION_DB_ROLES.getKey(), dbRoles);
        return this;
    }

    /**
     * Gets DB roles for an operation.
     *
     * @return list of DB roles
     */
    public Collection<String> getDBRoles() {
        return (Collection<String>) rawSettings.get(ClientConfigProperties.SESSION_DB_ROLES.getKey());
    }

    /**
     * Sets the comment that will be added to the query log record associated with the query.
     * @param logComment - comment to be added to the log
     * @return same instance of the builder
     */
    public QuerySettings logComment(String logComment) {
        this.logComment = logComment;
        if (logComment != null && !logComment.isEmpty()) {
            rawSettings.put(ClientConfigProperties.SETTING_LOG_COMMENT.getKey(), logComment);
        }
        return this;
    }

    private String logComment = null;

    public String getLogComment() {
        return logComment;
    }

    public static QuerySettings merge(QuerySettings source, QuerySettings override) {
        QuerySettings merged = new QuerySettings();
        if (source != null) {
            merged.rawSettings.putAll(source.rawSettings);
        }
        if (override != null && override != source) {// avoid copying the literally same object
            merged.rawSettings.putAll(override.rawSettings);
        }
        return merged;
    }
}
