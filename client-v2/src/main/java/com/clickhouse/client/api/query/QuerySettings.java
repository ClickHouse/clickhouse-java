package com.clickhouse.client.api.query;


import com.clickhouse.client.api.ClientSettings;
import com.clickhouse.config.ClickHouseBufferingMode;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

    public QuerySettings setQueryID(String queryID) {
        rawSettings.put("query_id", queryID);
        return this;
    }
    public String getQueryID() {
        return (String) rawSettings.get("query_id");
    }
    /**
     * Default buffer size in byte for both request and response.
     */
    public QuerySettings setBufferSize(Integer bufferSize) {
        ClientSettings.checkNotNull(bufferSize, "buffer_size");
        ClientSettings.checkPositive(bufferSize, "buffer_size");
        ClientSettings.checkRange(bufferSize, 2024, Integer.MAX_VALUE, "buffer_size");
        rawSettings.put("buffer_size", bufferSize);
        return this;
    }

    public Integer getBufferSize() {
        return (Integer) rawSettings.get("buffer_size");
    }

    /**
     * Number of times the buffer queue is filled up before increasing capacity of buffer queue.
     * Zero value means the queue length is fixed.
     */
    public QuerySettings setBufferQueueVariation(Integer bufferQueueVariation) {
        ClientSettings.checkNotNull(bufferQueueVariation, "buffer_queue_variation");
        ClientSettings.checkPositive(bufferQueueVariation, "buffer_queue_variation");
        rawSettings.put("buffer_queue_variation", bufferQueueVariation);
        return this;
    }

    public Integer getBufferQueueVariation() {
        return (Integer) rawSettings.get("buffer_queue_variation");
    }

    /**
     * Read buffer size in byte. Zero or negative value means same as buffer_size
     */
    public QuerySettings setReadBufferSize(Integer readBufferSize) {
        ClientSettings.checkNotNull(readBufferSize, "buffer_size");
        ClientSettings.checkPositive(readBufferSize, "buffer_size");
        ClientSettings.checkRange(readBufferSize, 2024, Integer.MAX_VALUE, "buffer_size");
        rawSettings.put("read_buffer_size", readBufferSize);
        return this;
    }

    public Integer getReadBufferSize() {
        return (Integer) rawSettings.get("read_buffer_size");
    }

    /**
     * Maximum request chunk size in byte. Zero or negative value means same as write_buffer_size
     */
    public QuerySettings setRequestChunkSize(Integer requestChunkSize) {
        ClientSettings.checkNotNull(requestChunkSize, "request_chunk_size");
        ClientSettings.checkPositive(requestChunkSize, "request_chunk_size");
        ClientSettings.checkRange(requestChunkSize, 2024, Integer.MAX_VALUE, "request_chunk_size");
        rawSettings.put("request_chunk_size", requestChunkSize);
        return this;
    }

    public Integer getRequestChunkSize() {
        return (Integer) rawSettings.get("request_chunk_size");
    }

    /**
     * Request buffering mode.
     * Possible values: "RESOURCE_EFFICIENT", "PERFORMANCE", "CUSTOM"
     * Custom mode is not used currently.
     * See {@link ClickHouseBufferingMode}
     */
    public QuerySettings setRequestBuffering(String requestBuffering) {
        ClientSettings.checkNotNull(requestBuffering, "request_buffering");
        ClientSettings.checkValueFromSet(requestBuffering, "request_buffering", REQUEST_BUFFERING_VALUES);
        rawSettings.put("request_buffering", requestBuffering);
        return this;
    }

    private static final Set<String> REQUEST_BUFFERING_VALUES = ClientSettings.whiteList(
            "RESOURCE_EFFICIENT", "PERFORMANCE", "CUSTOM");

    public String getRequestBuffering() {
        return (String) rawSettings.get("request_buffering");
    }

    /**
     * Response buffering mode.
     */
    public QuerySettings setResponseBuffering(String responseBuffering) {
        rawSettings.put("response_buffering", responseBuffering);
        return this;
    }

    public String getResponseBuffering() {
        return (String) rawSettings.get("response_buffering");
    }

    /**
     * Whether the server will compress response it sends to client.
     */
    public QuerySettings setCompress(Boolean compress) {
        rawSettings.put("compress", compress);
        return this;
    }

    public Boolean getCompress() {
        return (Boolean) rawSettings.get("compress");
    }

    /**
     * Algorithm used for server to compress response.
     */
    public QuerySettings setCompressAlgorithm(String compressAlgorithm) {
        rawSettings.put("compress_algorithm", compressAlgorithm);
        return this;
    }

    public String getCompressAlgorithm() {
        return (String) rawSettings.get("compress_algorithm");
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
     * Connection timeout in milliseconds. It's also used for waiting a connection being closed.
     */
    public QuerySettings setConnectTimeout(Integer connectTimeout) {
        rawSettings.put("connect_timeout", connectTimeout);
        return this;
    }

    public Integer getConnectTimeout() {
        return (Integer) rawSettings.get("connect_timeout");
    }

    /**
     * Default database.
     */
    public QuerySettings setDatabase(String database) {
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
        rawSettings.put("format", format);
        return this;
    }

    public String getFormat() {
        return (String) rawSettings.get("format");
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
     * Maximum number of times retry can happen for a request. Zero or negative value means no retry.
     */
    public QuerySettings setRetry(Integer retry) {
        rawSettings.put("retry", retry);
        return this;
    }

    public Integer getRetry() {
        return (Integer) rawSettings.get("retry");
    }

    /**
     * Whether to repeat execution when session is locked. Until timed out(according to 'session_timeout' or 'connect_timeout').
     */
    public QuerySettings setRepeatOnSessionLock(Boolean repeatOnSessionLock) {
        rawSettings.put("repeat_on_session_lock", repeatOnSessionLock);
        return this;
    }

    public Boolean getRepeatOnSessionLock() {
        return (Boolean) rawSettings.get("repeat_on_session_lock");
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
        rawSettings.put("session_timeout", sessionTimeout);
        return this;
    }

    public Integer getSessionTimeout() {
        return (Integer) rawSettings.get("session_timeout");
    }

    /**
     * Socket timeout in milliseconds.
     */
    public QuerySettings setSocketTimeout(Integer socketTimeout) {
        rawSettings.put("socket_timeout", socketTimeout);
        return this;
    }

    public Integer getSocketTimeout() {
        return (Integer) rawSettings.get("socket_timeout");
    }

    /**
     * Whether allows for the reuse of local addresses and ports. Only works for client using custom Socket(e.g. TCP client or HTTP provider with custom SocketFactory etc.).
     */
    public QuerySettings setSocketReuseaddr(Boolean socketReuseaddr) {
        rawSettings.put("socket_reuseaddr", socketReuseaddr);
        return this;
    }

    public Boolean getSocketReuseaddr() {
        return (Boolean) rawSettings.get("socket_reuseaddr");
    }

    /**
     * Whether to enable keep-alive packets for a socket connection. Only works for client using custom Socket.
     */
    public QuerySettings setSocketKeepalive(Boolean socketKeepalive) {
        rawSettings.put("socket_keepalive", socketKeepalive);
        return this;
    }

    public Boolean getSocketKeepalive() {
        return (Boolean) rawSettings.get("socket_keepalive");
    }

    /**
     * Seconds to wait while data is being transmitted before closing the socket. Use negative number to disable the option. Only works for client using custom Socket(e.g. TCP client or HTTP provider with custom SocketFactory etc.).
     */
    public QuerySettings setSocketLinger(Integer socketLinger) {
        rawSettings.put("socket_linger", socketLinger);
        return this;
    }

    public Integer getSocketLinger() {
        return (Integer) rawSettings.get("socket_linger");
    }

    /**
     * Socket IP_TOS option which indicates IP package priority. Only works for client using custom Socket.
     */
    public QuerySettings setSocketIpTos(Integer socketIpTos) {
        rawSettings.put("socket_ip_tos", socketIpTos);
        return this;
    }

    public Integer getSocketIpTos() {
        return (Integer) rawSettings.get("socket_ip_tos");
    }

    /**
     *
     */
    public QuerySettings setSocketTcpNodelay(Boolean socketTcpNodelay) {
        rawSettings.put("socket_tcp_nodelay", socketTcpNodelay);
        return this;
    }

    public Boolean getSocketTcpNodelay() {
        return (Boolean) rawSettings.get("socket_tcp_nodelay");
    }

    /**
     * Size of the socket receive buffer in bytes. Only works for client using custom Socket.
     */
    public QuerySettings setSocketRcvbuf(Integer socketRcvbuf) {
        rawSettings.put("socket_rcvbuf", socketRcvbuf);
        return this;
    }

    public Integer getSocketRcvbuf() {
        return (Integer) rawSettings.get("socket_rcvbuf");
    }

    /**
     * Size of the socket send buffer in bytes. Only works for client using custom Socket.
     */
    public QuerySettings setSocketSndbuf(Integer socketSndbuf) {
        rawSettings.put("socket_sndbuf", socketSndbuf);
        return this;
    }

    public Integer getSocketSndbuf() {
        return (Integer) rawSettings.get("socket_sndbuf");
    }

    /**
     * Whether to use server time zone. On connection init select timezone() will be executed
     */
    public QuerySettings setUseServerTimeZone(Boolean useServerTimeZone) {
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
        rawSettings.put("use_server_time_zone_for_dates", useServerTimeZoneForDates);
        return this;
    }

    public Boolean getUseServerTimeZoneForDates() {
        return (Boolean) rawSettings.get("use_server_time_zone_for_dates");
    }

    /**
     * Custom HTTP headers.
     */
    public QuerySettings setCustomHttpHeaders(Map customHttpHeaders) {
        rawSettings.put("custom_http_headers", customHttpHeaders);
        return this;
    }

    public Map getCustomHttpHeaders() {
        return (Map) rawSettings.get("custom_http_headers");
    }

    /**
     * Custom HTTP query parameters.
     */
    public QuerySettings setCustomHttpParams(Map customHttpParams) {
        rawSettings.put("custom_http_params", customHttpParams);
        return this;
    }

    public Map getCustomHttpParams() {
        return (Map) rawSettings.get("custom_http_params");
    }

    /**
     * Default server response which is used for validating connection.
     */
    public QuerySettings setHttpServerDefaultResponse(String httpServerDefaultResponse) {
        rawSettings.put("http_server_default_response", httpServerDefaultResponse);
        return this;
    }

    public String getHttpServerDefaultResponse() {
        return (String) rawSettings.get("http_server_default_response");
    }

    /**
     * Whether to use keep-alive or not
     */
    public QuerySettings setHttpKeepAlive(Boolean httpKeepAlive) {
        rawSettings.put("http_keep_alive", httpKeepAlive);
        return this;
    }

    public Boolean getHttpKeepAlive() {
        return (Boolean) rawSettings.get("http_keep_alive");
    }

    /**
     * Whether to receive information about the progress of a query in response headers.
     */
    public QuerySettings setReceiveQueryProgress(Boolean receiveQueryProgress) {
        rawSettings.put("receive_query_progress", receiveQueryProgress);
        return this;
    }

    public Boolean getReceiveQueryProgress() {
        return (Boolean) rawSettings.get("receive_query_progress");
    }
}
