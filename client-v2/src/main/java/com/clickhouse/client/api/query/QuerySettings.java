package com.clickhouse.client.api.query;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.internal.CommonSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.data.ClickHouseFormat;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

/**
 * <p>Query settings class represents a set of settings that can be used to customize query execution.</p>
 */
public class QuerySettings {

    public static final int MINIMAL_READ_BUFFER_SIZE = 8192;

    private final CommonSettings settings;

    public QuerySettings(Map<String, Object> settings) {
        this.settings = new CommonSettings();
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            this.settings.setOption(entry.getKey(), entry.getValue());
        }
    }

    public QuerySettings() {
        this.settings = new CommonSettings();
    }

    private QuerySettings(CommonSettings settings) {
        this.settings = settings;
    }

    /**
     * Sets a configuration option. This method can be used to set any configuration option.
     * There is no specific validation is done on the key or value.
     *
     * @param option - configuration option name
     * @param value  - configuration option value
     */
    public QuerySettings setOption(String option, Object value) {
        settings.setOption(option, value);
        return this;
    }

    public QuerySettings resetOption(String option) {
        settings.resetOption(option);
        return this;
    }

    /**
     * Gets a configuration option.
     *
     * @param option - configuration option name
     * @return configuration option value
     */
    public Object getOption(String option) {
        return settings.getOption(option);
    }

    /**
     * Get raw settings. Returns reference to internal map, so any changes will affect this object.
     *
     * @return all settings map
     */
    public Map<String, Object> getAllSettings() {
        return settings.getAllSettings();
    }

    /**
     * Sets the query id. This id will be sent to the server and can be used to identify the query.
     */
    public QuerySettings setQueryId(String queryId) {
        settings.setQueryId(queryId);
        return this;
    }

    public String getQueryId() {
        return settings.getQueryId();
    }

    /**
     * Read buffer is used for reading data from a server. Size is in bytes.
     * Minimal value is {@value MINIMAL_READ_BUFFER_SIZE} bytes.
     */
    public QuerySettings setReadBufferSize(Integer size) {
        ValidationUtils.checkNotNull(size, "read_buffer_size");
        ValidationUtils.checkRange(size, MINIMAL_READ_BUFFER_SIZE, Integer.MAX_VALUE, "read_buffer_size");
        settings.setOption("read_buffer_size", size);
        return this;
    }

    public Integer getReadBufferSize() {
        return (Integer) settings.getOption("read_buffer_size");
    }

    /**
     * Sets output format for a server response.
     */
    public QuerySettings setFormat(ClickHouseFormat format) {
        settings.setOption("format", format);
        return this;
    }

    public ClickHouseFormat getFormat() {
        return (ClickHouseFormat) settings.getOption("format");
    }

    /**
     * Maximum query execution time in seconds on server. 0 means no limit.
     * If query is not finished in this time then server will send an exception.
     */
    public QuerySettings setMaxExecutionTime(Integer maxExecutionTime) {
        serverSetting(ServerSettings.MAX_EXECUTION_TIME, String.valueOf(maxExecutionTime));
        return this;
    }

    public Integer getMaxExecutionTime() {
        String val = (String) settings.getOption(
                ClientConfigProperties.serverSetting(ServerSettings.MAX_EXECUTION_TIME));
        return val == null ? null : Integer.valueOf(val);
    }

    /**
     * Sets database to be used for a request.
     */
    public QuerySettings setDatabase(String database) {
        settings.setDatabase(database);
        return this;
    }

    public String getDatabase() {
        return settings.getDatabase();
    }

    /**
     * Requests the server to wait for the and of the query before sending response. Useful for getting accurate summary.
     */
    public QuerySettings waitEndOfQuery(Boolean waitEndOfQuery) {
        serverSetting(ServerSettings.WAIT_END_OF_QUERY, waitEndOfQuery ? "1" : "0");
        return this;
    }

    public QuerySettings setUseServerTimeZone(Boolean useServerTimeZone) {
        if (settings.hasOption(ClientConfigProperties.USE_TIMEZONE.getKey())) {
            throw new ValidationUtils.SettingsValidationException(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey(),
                    "Cannot set both use_time_zone and use_server_time_zone");
        }
        settings.setOption(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey(), useServerTimeZone);
        return this;
    }

    public Boolean getUseServerTimeZone() {
        return (Boolean) settings.getOption(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey());
    }

    public QuerySettings setUseTimeZone(String timeZone) {
        if (settings.hasOption(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey())) {
            throw new ValidationUtils.SettingsValidationException(ClientConfigProperties.USE_TIMEZONE.getKey(),
                    "Cannot set both use_time_zone and use_server_time_zone");
        }
        settings.setOption(ClientConfigProperties.USE_TIMEZONE.getKey(), TimeZone.getTimeZone(timeZone));
        return this;
    }

    public TimeZone getServerTimeZone() {
        return (TimeZone) settings.getOption(ClientConfigProperties.SERVER_TIMEZONE.getKey());
    }

    /**
     * Defines list of headers that should be sent with current request. The Client will use a header value
     * defined in {@code headers} instead of any other.
     *
     * @param key   - header name.
     * @param value - header value.
     * @return same instance of the builder
     * @see Client.Builder#httpHeaders(Map)
     */
    public QuerySettings httpHeader(String key, String value) {
        settings.httpHeader(key, value);
        return this;
    }

    /**
     * {@see #httpHeader(String, String)} but for multiple values.
     *
     * @param key    - name of the header
     * @param values - collection of values
     * @return same instance of the builder
     */
    public QuerySettings httpHeader(String key, Collection<String> values) {
        settings.httpHeader(key, values);
        return this;
    }

    /**
     * {@see #httpHeader(String, String)} but for multiple headers.
     *
     * @param headers - map of headers
     * @return same instance of the builder
     */
    public QuerySettings httpHeaders(Map<String, String> headers) {
        settings.httpHeaders(headers);
        return this;
    }

    /**
     * Defines list of server settings that should be sent with each request. The Client will use a setting value
     * defined in {@code settings} instead of any other.
     * Operation settings may override these values.
     *
     * @param name  - name of the setting
     * @param value - value of the setting
     * @return same instance of the builder
     * @see Client.Builder#serverSetting(String, Collection)
     */
    public QuerySettings serverSetting(String name, String value) {
        settings.serverSetting(name, value);
        return this;
    }

    /**
     * {@see #serverSetting(String, String)} but for multiple values.
     *
     * @param name   - name of the setting without special prefix
     * @param values - collection of values
     * @return same instance of the builder
     */
    public QuerySettings serverSetting(String name, Collection<String> values) {
        settings.serverSetting(name, values);
        return this;
    }

    /**
     * Sets DB roles for an operation. Roles that were set by {@link Client#setDBRoles(Collection)} will be overridden.
     *
     * @param dbRoles - list of role to use with an operation
     */
    public QuerySettings setDBRoles(Collection<String> dbRoles) {
        settings.setDBRoles(dbRoles);
        return this;
    }

    /**
     * Gets DB roles for an operation.
     *
     * @return list of DB roles
     */
    public Collection<String> getDBRoles() {
        return settings.getDBRoles();
    }

    /**
     * Sets the comment that will be added to the query log record associated with the query.
     *
     * @param logComment - comment to be added to the log
     * @return same instance of the builder
     */
    public QuerySettings logComment(String logComment) {
        settings.logComment(logComment);
        return this;
    }

    public String getLogComment() {
        return settings.getLogComment();
    }

    /**
     * Sets a network operation timeout.
     * @param timeout
     * @param unit
     */
    public void setNetworkTimeout(long timeout, ChronoUnit unit) {
        settings.setNetworkTimeout(timeout, unit);
    }

    /**
     * Returns network timeout. Zero value is returned if no timeout is set.
     * @return timeout in ms.
     */
    public Long getNetworkTimeout() {
        return settings.getNetworkTimeout();
    }

    public static QuerySettings merge(QuerySettings source, QuerySettings override) {
        CommonSettings mergedSettings = source.settings.copyAndMerge(override.settings);
        return new QuerySettings(mergedSettings);
    }
}
