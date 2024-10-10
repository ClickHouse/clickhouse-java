package com.clickhouse.client.api.query;


import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientSettings;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;

import javax.management.Query;
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
        rawSettings.put("wait_end_of_query", waitEndOfQuery);
        return this;
    }

    public QuerySettings setUseServerTimeZone(Boolean useServerTimeZone) {
        if (rawSettings.containsKey(ClickHouseClientOption.USE_TIME_ZONE.getKey())) {
            throw new ValidationUtils.SettingsValidationException("use_server_timezone",
                    "Cannot set both use_time_zone and use_server_time_zone");
        }
        rawSettings.put("use_server_time_zone", useServerTimeZone);
        return this;
    }

    public Boolean getUseServerTimeZone() {
        return (Boolean) rawSettings.get("use_server_time_zone");
    }

    public QuerySettings setUseTimeZone(String timeZone) {
        if (rawSettings.containsKey(ClickHouseClientOption.USE_SERVER_TIME_ZONE.getKey())) {
            throw new ValidationUtils.SettingsValidationException("use_time_zone",
                    "Cannot set both use_time_zone and use_server_time_zone");
        }
        rawSettings.put("use_time_zone", timeZone);
        return this;
    }

    public TimeZone getServerTimeZone() {
        return (TimeZone) rawSettings.get(ClickHouseClientOption.SERVER_TIME_ZONE.getKey());
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
        rawSettings.put(ClientSettings.HTTP_HEADER_PREFIX + key, value);
        return this;
    }

    /**
     * {@see #httpHeader(String, String)} but for multiple values.
     * @param key - name of the header
     * @param values - collection of values
     * @return same instance of the builder
     */
    public QuerySettings httpHeader(String key, Collection<String> values) {
        rawSettings.put(ClientSettings.HTTP_HEADER_PREFIX + key, ClientSettings.commaSeparated(values));
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
        rawSettings.put(ClientSettings.SERVER_SETTING_PREFIX + name, value);
        return this;
    }

    /**
     * {@see #serverSetting(String, String)} but for multiple values.
     * @param name - name of the setting without special prefix
     * @param values - collection of values
     * @return same instance of the builder
     */
    public QuerySettings serverSetting(String name, Collection<String> values) {
        rawSettings.put(ClientSettings.SERVER_SETTING_PREFIX + name, ClientSettings.commaSeparated(values));
        return this;
    }

    /**
     * Sets the comment that will be added to the query log record associated with the query.
     * @param logComment - comment to be added to the log
     * @return same instance of the builder
     */
    public QuerySettings logComment(String logComment) {
        this.logComment = logComment;
        if (logComment != null && !logComment.isEmpty()) {
            rawSettings.put(ClientSettings.SERVER_SETTING_PREFIX + "log_comment", logComment);
        }
        return this;
    }

    private String logComment = null;

    public String getLogComment() {
        return logComment;
    }
}
