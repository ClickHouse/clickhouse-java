package com.clickhouse.client.api.query;


import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.data.ClickHouseFormat;

import java.util.HashMap;
import java.util.Map;

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
}
