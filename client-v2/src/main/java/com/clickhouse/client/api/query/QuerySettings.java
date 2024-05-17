package com.clickhouse.client.api.query;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.internal.ValidationUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class QuerySettings {

    private Map<String, Object> rawSettings;

    private ExecutorService executorService;

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

    public String getQueryID() {
        return (String) rawSettings.get("query_id");
    }

    /**
     * Sets executor service that will be used for asynchronous calls (ex. waiting for query result).
     * If not set, single thread executor will be used.
     * @param executorService
     * @return
     */
    public QuerySettings setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    /**
     * Default buffer size in byte for both request and response.
     */
    public QuerySettings setBufferSize(Integer bufferSize) {
        ValidationUtils.checkNotNull(bufferSize, "buffer_size");
        ValidationUtils.checkPositive(bufferSize, "buffer_size");
        ValidationUtils.checkRange(bufferSize, 2024, Integer.MAX_VALUE, "buffer_size");
        rawSettings.put("buffer_size", bufferSize);
        return this;
    }

    public Integer getBufferSize() {
        return (Integer) rawSettings.get("buffer_size");
    }

    /**
     * Read buffer size in byte. Zero or negative value means same as buffer_size
     */
    public QuerySettings setReadBufferSize(Integer readBufferSize) {
        ValidationUtils.checkNotNull(readBufferSize, "buffer_size");
        ValidationUtils.checkPositive(readBufferSize, "buffer_size");
        ValidationUtils.checkRange(readBufferSize, 2024, Integer.MAX_VALUE, "buffer_size");
        rawSettings.put("read_buffer_size", readBufferSize);
        return this;
    }

    public Integer getReadBufferSize() {
        return (Integer) rawSettings.get("read_buffer_size");
    }

    /**
     * Write buffer size in byte. Zero or negative value means same as buffer_size
     */
    public QuerySettings setWriteBufferSize(Integer writeBufferSize) {
        ValidationUtils.checkNotNull(writeBufferSize, "buffer_size");
        ValidationUtils.checkPositive(writeBufferSize, "buffer_size");
        ValidationUtils.checkRange(writeBufferSize, 2024, Integer.MAX_VALUE, "buffer_size");
        rawSettings.put("write_buffer_size", writeBufferSize);
        return this;
    }

    public Integer getWriteBufferSize() {
        return (Integer) rawSettings.get("write_buffer_size");
    }

    /**
     * Whether the server will compress response it sends to client.
     */
    public QuerySettings setCompress(Boolean compress) {
        ValidationUtils.checkNotNull(compress, "compress");
        rawSettings.put("compress", compress);
        return this;
    }

    public Boolean getCompress() {
        return (Boolean) rawSettings.get("compress");
    }

    /**
     * Whether the server will decompress request from client.
     */
    public QuerySettings setDecompress(Boolean decompress) {
        ValidationUtils.checkNotNull(decompress, "decompress");
        rawSettings.put("decompress", decompress);
        return this;
    }

    public Boolean getDecompress() {
        return (Boolean) rawSettings.get("decompress");
    }

    /**
     * Algorithm used for server to compress response.
     *
     */
    public QuerySettings setCompressAlgorithm(String compressAlgorithm) {
        ValidationUtils.checkNonBlank(compressAlgorithm, "compress_algorithm");
        ValidationUtils.checkValueFromSet(compressAlgorithm, "compress_algorithm",
                Client.getCompressAlgorithms());
        rawSettings.put("compress_algorithm", compressAlgorithm);
        return this;
    }

    public String getCompressAlgorithm() {
        return (String) rawSettings.get("compress_algorithm");
    }

    /**
     * Algorithm for server to decompress request.
     */
    public QuerySettings setDecompressAlgorithm(String decompressAlgorithm) {
        ValidationUtils.checkNonBlank(decompressAlgorithm, "decompress_algorithm");
        ValidationUtils.checkValueFromSet(decompressAlgorithm, "decompress_algorithm",
                Client.getCompressAlgorithms());
        rawSettings.put("decompress_algorithm", decompressAlgorithm);
        return this;
    }

    public String getDecompressAlgorithm() {
        return (String) rawSettings.get("decompress_algorithm");
    }

    /**
     * Compression level for response. -1 standards for default
     */
    public QuerySettings setCompressLevel(Integer compressLevel) {
        rawSettings.put("compress_level", compressLevel);
        return this;
    }

    public Integer getCompressLevel() {
        return (Integer) rawSettings.get("compress_level");
    }

    /**
     * Compression level for request. -1 standards for default
     */
    public QuerySettings setDecompressLevel(Integer decompressLevel) {
        rawSettings.put("decompress_level", decompressLevel);
        return this;
    }

    public Integer getDecompressLevel() {
        return (Integer) rawSettings.get("decompress_level");
    }

    /**
     * Default database.
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
     * Default format.
     */
    public QuerySettings setFormat(String format) {
        ValidationUtils.checkNonBlank(format, "format");
        ValidationUtils.checkValueFromSet(format, "format", Client.getOutputFormats());
        rawSettings.put("format", format);
        return this;
    }

    public String getFormat() {
        return (String) rawSettings.get("format");
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
     * Whether to use timezone from server on Date parsing in getDate(). If false Date returned is a wrapper of a timestamp at start of the day in client timezone. If true - at start of the day in server or use_time_zone timezone.
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


    // TODO: it should be dynamically loaded from classpath

}
