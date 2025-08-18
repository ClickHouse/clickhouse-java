package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Class representing very common logic across all setting objects
 * like setting database, getting option
 *
 */
public class CommonSettings {

    private String operationId;
    private String logComment;
    protected Map<String, Object> settings;

    public CommonSettings() {
        settings = new HashMap<>();
    }

    /**
     * Gets a configuration option.
     *
     * @param option - configuration option name
     * @return configuration option value
     */
    public Object getOption(String option) {
        return settings.get(option);
    }

    public boolean hasOption(String option) {
        return settings.containsKey(option);
    }

    /**
     * Sets a configuration option. This method can be used to set any configuration option.
     * There is no specific validation is done on the key or value.
     *
     * @param option - configuration option name
     * @param value  - configuration option value
     */
    public CommonSettings setOption(String option, Object value) {
        settings.put(option, value);
        if (option.equals(ClientConfigProperties.PRODUCT_NAME.getKey())) {
            settings.put(ClientConfigProperties.CLIENT_NAME.getKey(), value);
        }
        return this;
    }

    public CommonSettings resetOption(String option) {
        settings.remove(option);
        return this;
    }

    /**
     * Get all settings as an unmodifiable map.
     *
     * @return all settings
     */
    public Map<String, Object> getAllSettings() {
        return settings;
    }

    public String getQueryId() {
        return (String) settings.get(ClientConfigProperties.QUERY_ID.getKey());
    }

    /**
     * Sets the query id. This id will be sent to the server and can be used to identify the query.
     */
    public CommonSettings setQueryId(String queryId) {
        settings.put(ClientConfigProperties.QUERY_ID.getKey(), queryId);
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
    public CommonSettings setOperationId(String operationId) {
        this.operationId = operationId;
        return this;
    }

    /**
     * Sets database to be used for a request.
     */
    public CommonSettings setDatabase(String database) {
        ValidationUtils.checkNonBlank(database, ClientConfigProperties.DATABASE.getKey());
        settings.put(ClientConfigProperties.DATABASE.getKey(), database);
        return this;
    }

    public String getDatabase() {
        return (String) settings.get(ClientConfigProperties.DATABASE.getKey());
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
    public CommonSettings httpHeader(String key, String value) {
        settings.put(ClientConfigProperties.httpHeader(key), value);
        return this;
    }

    /**
     * {@see #httpHeader(String, String)} but for multiple values.
     * @param key - name of the header
     * @param values - collection of values
     * @return same instance of the builder
     */
    public CommonSettings httpHeader(String key, Collection<String> values) {
        settings.put(ClientConfigProperties.httpHeader(key), ClientConfigProperties.commaSeparated(values));
        return this;
    }

    /**
     * {@see #httpHeader(String, String)} but for multiple headers.
     * @param headers - map of headers
     * @return same instance of the builder
     */
    public CommonSettings httpHeaders(Map<String, String> headers) {
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
    public CommonSettings serverSetting(String name, String value) {
        settings.put(ClientConfigProperties.serverSetting(name), value);
        return this;
    }

    /**
     * {@see #serverSetting(String, String)} but for multiple values.
     * @param name - name of the setting without special prefix
     * @param values - collection of values
     * @return same instance of the builder
     */
    public CommonSettings serverSetting(String name, Collection<String> values) {
        settings.put(ClientConfigProperties.serverSetting(name), ClientConfigProperties.commaSeparated(values));
        return this;
    }

    /**
     * Sets DB roles for an operation. Roles that were set by {@link Client#setDBRoles(Collection)} will be overridden.
     *
     * @param dbRoles
     */
    public CommonSettings setDBRoles(Collection<String> dbRoles) {
        settings.put(ClientConfigProperties.SESSION_DB_ROLES.getKey(), dbRoles);
        return this;
    }

    /**
     * Gets DB roles for an operation.
     *
     * @return list of DB roles
     */
    public Collection<String> getDBRoles() {
        return (Collection<String>) settings.get(ClientConfigProperties.SESSION_DB_ROLES.getKey());
    }

    /**
     * Sets the comment that will be added to the query log record associated with the query.
     * @param logComment - comment to be added to the log
     * @return same instance of the builder
     */
    public CommonSettings logComment(String logComment) {
        this.logComment = logComment;
        if (logComment != null && !logComment.isEmpty()) {
            settings.put(ClientConfigProperties.SETTING_LOG_COMMENT.getKey(), logComment);
        }
        return this;
    }

    public String getLogComment() {
        return logComment;
    }

    public CommonSettings copyAndMerge(CommonSettings override) {
        CommonSettings copy = new  CommonSettings();
        copy.settings.putAll(settings);
        copy.logComment = logComment;
        copy.settings.putAll(override.settings);

        return copy;
    }
}
