package com.clickhouse.client.api.insert;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.internal.CommonSettings;
import org.apache.hc.core5.http.HttpHeaders;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;

public class InsertSettings {
    private static final int DEFAULT_INPUT_STREAM_BATCH_SIZE = 8196;

    private int inputStreamCopyBufferSize;
    CommonSettings settings;

    public InsertSettings() {
        settings = new CommonSettings();
        setDefaults();
    }

    public InsertSettings(Map<String, Object> settings) {
        this.settings = new CommonSettings();
        setDefaults();
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            this.settings.setOption(entry.getKey(), entry.getValue());
        }
    }

    private InsertSettings(CommonSettings settings) {
        this.settings = settings;
        setDefaults();
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
        return settings.getOption(option);
    }

    /**
     * Sets a configuration option. This method can be used to set any configuration option.
     * There is no specific validation is done on the key or value.
     *
     * @param option - configuration option name
     * @param value  - configuration option value
     */
    public InsertSettings setOption(String option, Object value) {
        settings.setOption(option, value);
        return this;
    }

    /**
     * Get all settings as an unmodifiable map.
     *
     * @return all settings
     */
    public Map<String, Object> getAllSettings() {
        return settings.getAllSettings();
    }

    /**
     * Sets the deduplication token. This token will be sent to the server and can be used to identify the query.
     *
     * @param token - deduplication token
     * @return
     */
    public InsertSettings setDeduplicationToken(String token) {
        serverSetting("insert_deduplication_token", token);
        return this;
    }

    public String getQueryId() {
        return settings.getQueryId();
    }

    /**
     * Sets the query id. This id will be sent to the server and can be used to identify the query.
     */
    public InsertSettings setQueryId(String queryId) {
        settings.setQueryId(queryId);
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
        return settings.getOperationId();
    }

    /**
     * Operation id. Used internally to register new operation.
     * Should not be called directly.
     *
     * @param operationId - operation id
     */
    public InsertSettings setOperationId(String operationId) {
        settings.setOperationId(operationId);
        return this;
    }

    /**
     * Sets database to be used for a request.
     */
    public InsertSettings setDatabase(String database) {
        settings.setDatabase(database);
        return this;
    }

    public String getDatabase() {
        return settings.getDatabase();
    }

    /**
     * Client request compression. If set to true client will compress the request.
     *
     * @param enabled - indicates if client request compression is enabled
     */
    public InsertSettings compressClientRequest(boolean enabled) {
        settings.setOption(ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getKey(), enabled);
        return this;
    }

    public InsertSettings useHttpCompression(boolean enabled) {
        settings.setOption(ClientConfigProperties.USE_HTTP_COMPRESSION.getKey(), enabled);
        return this;
    }

    /**
     * Sets flag that indicates if application provides already compressed data
     *
     * @param enabled - if application provides compressed data
     */
    public InsertSettings appCompressedData(boolean enabled, String compressionMethod) {
        settings.setOption(ClientConfigProperties.APP_COMPRESSED_DATA.getKey(), enabled);
        useHttpCompression(true);
        httpHeader(HttpHeaders.CONTENT_ENCODING, compressionMethod);
        return this;
    }

    /**
     *
     * @return true if client compression is enabled
     * @deprecated because of typo
     */
    public boolean isClientRequestEnabled() {
        return isClientCompressionEnabled();
    }

    /**
     * Returns indication if client request should be compressed (client side compression).
     *
     * @return true if client compression is enabled
     */
    public boolean isClientCompressionEnabled() {
        return (boolean) settings.getOption(
                ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getKey(),
                false
        );
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
    public InsertSettings httpHeader(String key, String value) {
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
    public InsertSettings httpHeader(String key, Collection<String> values) {
        settings.httpHeader(key, values);
        return this;
    }

    /**
     * {@see #httpHeader(String, String)} but for multiple headers.
     *
     * @param headers - map of headers
     * @return same instance of the builder
     */
    public InsertSettings httpHeaders(Map<String, String> headers) {
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
    public InsertSettings serverSetting(String name, String value) {
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
    public InsertSettings serverSetting(String name, Collection<String> values) {
        settings.serverSetting(name, values);
        return this;
    }

    /**
     * Sets DB roles for an operation. Roles that were set by {@link Client#setDBRoles(Collection)} will be overridden.
     *
     * @param dbRoles
     */
    public InsertSettings setDBRoles(Collection<String> dbRoles) {
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
    public InsertSettings logComment(String logComment) {
        settings.logComment(logComment);
        return this;
    }

    public String getLogComment() {
        return settings.getLogComment();
    }

    public static InsertSettings merge(InsertSettings source, InsertSettings override) {
        CommonSettings mergedSettings = source.settings.copyAndMerge(override.settings);
        return new InsertSettings(mergedSettings);
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
}
