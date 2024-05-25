package com.clickhouse.client.api.query;


import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.data.ClickHouseFormat;

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
    public Map<String, Object> getAllSettings() {
        return rawSettings;
    }

    public QuerySettings setQueryID(String queryID) {
        ValidationUtils.checkNonBlank(queryID, "query_id");
        rawSettings.put("query_id", queryID);
        return this;
    }

    public String getQueryId() {
        return (String) rawSettings.get("query_id");
    }

    /**
     * Sets size of a buffer for reading data from server in bytes.
     *
     */
    public QuerySettings setBufferSize(Integer bufferSize) {
        ValidationUtils.checkNotNull(bufferSize, "buffer_size");
        ValidationUtils.checkPositive(bufferSize, "buffer_size");
        ValidationUtils.checkRange(bufferSize, 2024, Integer.MAX_VALUE, "buffer_size");
        rawSettings.put("buffer_size", bufferSize);
        return this;
    }

    /**
     * Get buffer size in byte
     * @return buffer size in bytes
     */
    public Integer getBufferSize() {
        return (Integer) rawSettings.get("buffer_size");
    }

    /**
     * Sets output format for response.
     */
    public QuerySettings setFormat(ClickHouseFormat format) {
        rawSettings.put("format", format);
        return this;
    }

    public ClickHouseFormat getFormat() {
        return (ClickHouseFormat) rawSettings.get("format");
    }

    /**
     * Maximum query execution time in seconds. 0 means no limit.
     */
    public QuerySettings setMaxExecutionTime(Integer maxExecutionTime) {
        rawSettings.put("max_execution_time", maxExecutionTime);
        return this;
    }

    public Integer getMaxExecutionTime() {
        return (Integer) rawSettings.get("max_execution_time");
    }

    /**
     * Method to rename response columns.
     */
    public QuerySettings setRenameResponseColumn(String renameResponseColumn) {
        rawSettings.put("rename_response_column", renameResponseColumn);
        return this;
    }

    public String getRenameResponseColumn() {
        return (String) rawSettings.get("rename_response_column");
    }

    /**
     * Session id
     */
    public QuerySettings setSessionId(String sessionId) {
        rawSettings.put("session_id", sessionId);
        return this;
    }

    public String getSessionId() {
        return (String) rawSettings.get("session_id");
    }

    /**
     * Whether to check if existence of session id.
     */
    public QuerySettings setSessionCheck(Boolean sessionCheck) {
        ValidationUtils.checkNotNull(sessionCheck, "session_check");
        rawSettings.put("session_check", sessionCheck);
        return this;
    }

    public Boolean getSessionCheck() {
        return (Boolean) rawSettings.get("session_check");
    }

    /**
     * Session timeout in seconds. 0 or negative number means same as server default.
     */
    public QuerySettings setSessionTimeout(Integer sessionTimeout) {
        if (sessionTimeout != null) {
            ValidationUtils.checkPositive(sessionTimeout, "session_timeout");
        }
        rawSettings.put("session_timeout", sessionTimeout);
        return this;
    }

    public Integer getSessionTimeout() {
        return (Integer) rawSettings.get("session_timeout");
    }

    /**
     * Whether to use server time zone. On connection init select timezone() will be executed
     */
    public QuerySettings setUseServerTimeZone(Boolean useServerTimeZone) {
        ValidationUtils.checkNotNull(useServerTimeZone, "use_server_time_zone");
        rawSettings.put("use_server_time_zone", useServerTimeZone);
        return this;
    }

    public Boolean getUseServerTimeZone() {
        return (Boolean) rawSettings.get("use_server_time_zone");
    }

    /**
     * Whether to use timezone from server on Date parsing in getDate().
     * If false Date returned is a wrapper of a timestamp at start of the day in client timezone.
     * If true - at start of the day in server or use_time_zone timezone.
     */
    public QuerySettings setUseServerTimeZoneForDates(Boolean useServerTimeZoneForDates) {
        ValidationUtils.checkNotNull(useServerTimeZoneForDates, "use_server_time_zone_for_dates");
        rawSettings.put("use_server_time_zone_for_dates", useServerTimeZoneForDates);
        return this;
    }

    public Boolean getUseServerTimeZoneForDates() {
        return (Boolean) rawSettings.get("use_server_time_zone_for_dates");
    }

    /**
     * Custom HTTP headers.
     */
    public QuerySettings setCustomHttpHeaders(Map<String, String> customHttpHeaders) {
        rawSettings.put("custom_http_headers", customHttpHeaders);
        return this;
    }

    public Map<String, String> getCustomHttpHeaders() {
        return (Map<String, String>) rawSettings.get("custom_http_headers");
    }

    /**
     * Custom HTTP query parameters.
     */
    public QuerySettings setCustomHttpParams(Map<String, String> customHttpParams) {
        rawSettings.put("custom_http_params", customHttpParams);
        return this;
    }

    public Map<String, String> getCustomHttpParams() {
        return (Map<String, String>) rawSettings.get("custom_http_params");
    }
}
