package ru.yandex.clickhouse.settings;

import ru.yandex.clickhouse.util.apache.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class ClickHouseProperties {

    // connection settings
    private boolean async;
    private int bufferSize;
    private int apacheBufferSize;
    private int socketTimeout;
    private int connectionTimeout;
    private int dataTransferTimeout;
    private int keepAliveTimeout;
    private int timeToLiveMillis;
    private int defaultMaxPerRoute;
    private int maxTotal;


    // queries settings
    private Integer maxParallelReplicas;
    private String totalsMode;
    private String quotaKey;
    private Integer priority;
    private String database;
    private boolean compress;
    private boolean extremes;
    private Integer maxThreads;
    private Integer maxExecutionTime;
    private Integer maxBlockSize;
    private Integer maxRowsToGroupBy;
    private String profile;
    private String user;


    public ClickHouseProperties() {
        this(new Properties());
    }

    public ClickHouseProperties(Properties info) {
        // need casts for java 6
        this.async = (Boolean)getSetting(info, ClickHouseConnectionSettings.ASYNC);
        this.bufferSize = (Integer)getSetting(info, ClickHouseConnectionSettings.BUFFER_SIZE);
        this.apacheBufferSize = (Integer)getSetting(info, ClickHouseConnectionSettings.APACHE_BUFFER_SIZE);
        this.socketTimeout = (Integer)getSetting(info, ClickHouseConnectionSettings.SOCKET_TIMEOUT);
        this.connectionTimeout = (Integer)getSetting(info, ClickHouseConnectionSettings.CONNECTION_TIMEOUT);
        this.dataTransferTimeout = (Integer)getSetting(info, ClickHouseConnectionSettings.DATA_TRANSFER_TIMEOUT);
        this.keepAliveTimeout = (Integer)getSetting(info, ClickHouseConnectionSettings.KEEP_ALIVE_TIMEOUT);
        this.timeToLiveMillis = (Integer)getSetting(info, ClickHouseConnectionSettings.TIME_TO_LIVE_MILLIS);
        this.defaultMaxPerRoute = (Integer)getSetting(info, ClickHouseConnectionSettings.DEFAULT_MAX_PER_ROUTE);
        this.maxTotal = (Integer)getSetting(info, ClickHouseConnectionSettings.MAX_TOTAL);

        this.maxParallelReplicas = getSetting(info, ClickHouseQueryParam.MAX_PARALLEL_REPLICAS);
        this.totalsMode = getSetting(info, ClickHouseQueryParam.TOTALS_MODE);
        this.quotaKey = getSetting(info, ClickHouseQueryParam.QUOTA_KEY);
        this.priority = getSetting(info, ClickHouseQueryParam.PRIORITY);
        this.database = getSetting(info, ClickHouseQueryParam.DATABASE);
        this.compress = (Boolean)getSetting(info, ClickHouseQueryParam.COMPRESS);
        this.extremes = (Boolean)getSetting(info, ClickHouseQueryParam.EXTREMES);
        this.maxThreads = getSetting(info, ClickHouseQueryParam.MAX_THREADS);
        this.maxExecutionTime = getSetting(info, ClickHouseQueryParam.MAX_EXECUTION_TIME);
        this.maxBlockSize = getSetting(info, ClickHouseQueryParam.MAX_BLOCK_SIZE);
        this.maxRowsToGroupBy = getSetting(info, ClickHouseQueryParam.MAX_ROWS_TO_GROUP_BY);
        this.profile = getSetting(info, ClickHouseQueryParam.PROFILE);
        this.user = getSetting(info, ClickHouseQueryParam.USER);
    }

    public ClickHouseProperties(ClickHouseProperties properties) {
        setAsync(properties.async);
        setBufferSize(properties.bufferSize);
        setApacheBufferSize(properties.apacheBufferSize);
        setSocketTimeout(properties.socketTimeout);
        setConnectionTimeout(properties.connectionTimeout);
        setDataTransferTimeout(properties.dataTransferTimeout);
        setKeepAliveTimeout(properties.keepAliveTimeout);
        setTimeToLiveMillis(properties.timeToLiveMillis);
        setDefaultMaxPerRoute(properties.defaultMaxPerRoute);
        setMaxTotal(properties.maxTotal);
        setMaxParallelReplicas(properties.maxParallelReplicas);
        setTotalsMode(properties.totalsMode);
        setQuotaKey(properties.quotaKey);
        setPriority(properties.priority);
        setDatabase(properties.database);
        setCompress(properties.compress);
        setExtremes(properties.extremes);
        setMaxThreads(properties.maxThreads);
        setMaxExecutionTime(properties.maxExecutionTime);
        setMaxBlockSize(properties.maxBlockSize);
        setMaxRowsToGroupBy(properties.maxRowsToGroupBy);
        setProfile(properties.profile);
        setUser(properties.user);
    }

    public Map<ClickHouseQueryParam, String> buildParams(boolean ignoreDatabase){
        Map<ClickHouseQueryParam, String> params = new HashMap<ClickHouseQueryParam, String>();

        if (maxParallelReplicas != null) params.put(ClickHouseQueryParam.MAX_PARALLEL_REPLICAS, String.valueOf(maxParallelReplicas));
        if (maxRowsToGroupBy != null) params.put(ClickHouseQueryParam.MAX_ROWS_TO_GROUP_BY, String.valueOf(maxRowsToGroupBy));
        if (totalsMode != null) params.put(ClickHouseQueryParam.TOTALS_MODE, totalsMode);
        if (quotaKey != null) params.put(ClickHouseQueryParam.QUOTA_KEY, quotaKey);
        if (priority != null) params.put(ClickHouseQueryParam.PRIORITY, String.valueOf(priority));

        if (!StringUtils.isBlank(database) && !ignoreDatabase) params.put(ClickHouseQueryParam.DATABASE, getDatabase());

        if (compress) params.put(ClickHouseQueryParam.COMPRESS, "1");

        if (extremes) params.put(ClickHouseQueryParam.EXTREMES, "1");

        if (StringUtils.isBlank(profile)) {
            if (getMaxThreads() != null)
                params.put(ClickHouseQueryParam.MAX_THREADS, String.valueOf(maxThreads));
            // in seconds there
            params.put(ClickHouseQueryParam.MAX_EXECUTION_TIME, String.valueOf((maxExecutionTime != null? maxExecutionTime:(socketTimeout + dataTransferTimeout)) / 1000));
            if (getMaxBlockSize() != null) {
                params.put(ClickHouseQueryParam.MAX_BLOCK_SIZE, String.valueOf(getMaxBlockSize()));
            }
        } else {
            params.put(ClickHouseQueryParam.PROFILE, profile);
        }

        if (user != null) params.put(ClickHouseQueryParam.USER, user);

        return params;
    }


    private <T> T getSetting(Properties info, ClickHouseQueryParam param){
        return getSetting(info, param.getKey(), param.getDefaultValue(), param.getClazz());
    }

    private <T> T getSetting(Properties info, ClickHouseConnectionSettings settings){
        return getSetting(info, settings.getKey(), settings.getDefaultValue(), settings.getClazz());
    }

    @SuppressWarnings("unchecked")
    private <T> T getSetting(Properties info, String key, Object defaultValue, Class clazz){
        Object val = info.get(key);
        if (val == null)
            return (T)defaultValue;
        if ((clazz == int.class || clazz == Integer.class) && val instanceof String) {
            return (T) clazz.cast(Integer.valueOf((String) val));
        }
        if ((clazz == long.class || clazz == Long.class) && val instanceof String) {
            return (T) clazz.cast(Long.valueOf((String) val));
        }
        if ((clazz == boolean.class || clazz == Boolean.class) && val instanceof String) {
            return (T) clazz.cast(Boolean.valueOf((String) val));
        }
        return (T) clazz.cast(val);
    }


    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public boolean isCompress() {
        return compress;
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    public boolean isAsync() {
        return async;
    }

    public void setAsync(boolean async) {
        this.async = async;
    }

    public Integer getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(Integer maxThreads) {
        this.maxThreads = maxThreads;
    }

    public Integer getMaxBlockSize() {
        return maxBlockSize;
    }

    public void setMaxBlockSize(Integer maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getApacheBufferSize() {
        return apacheBufferSize;
    }

    public void setApacheBufferSize(int apacheBufferSize) {
        this.apacheBufferSize = apacheBufferSize;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getDataTransferTimeout() {
        return dataTransferTimeout;
    }

    public void setDataTransferTimeout(int dataTransferTimeout) {
        this.dataTransferTimeout = dataTransferTimeout;
    }

    public int getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    public void setKeepAliveTimeout(int keepAliveTimeout) {
        this.keepAliveTimeout = keepAliveTimeout;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public int getTimeToLiveMillis() {
        return timeToLiveMillis;
    }

    public void setTimeToLiveMillis(int timeToLiveMillis) {
        this.timeToLiveMillis = timeToLiveMillis;
    }

    public int getDefaultMaxPerRoute() {
        return defaultMaxPerRoute;
    }

    public void setDefaultMaxPerRoute(int defaultMaxPerRoute) {
        this.defaultMaxPerRoute = defaultMaxPerRoute;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public Integer getMaxParallelReplicas() {
        return maxParallelReplicas;
    }

    public void setMaxParallelReplicas(Integer maxParallelReplicas) {
        this.maxParallelReplicas = maxParallelReplicas;
    }

    public String getTotalsMode() {
        return totalsMode;
    }

    public void setTotalsMode(String totalsMode) {
        this.totalsMode = totalsMode;
    }

    public String getQuotaKey() {
        return quotaKey;
    }

    public void setQuotaKey(String quotaKey) {
        this.quotaKey = quotaKey;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public boolean isExtremes() {
        return extremes;
    }

    public void setExtremes(boolean extremes) {
        this.extremes = extremes;
    }

    public Integer getMaxExecutionTime() {
        return maxExecutionTime;
    }

    public void setMaxExecutionTime(Integer maxExecutionTime) {
        this.maxExecutionTime = maxExecutionTime;
    }

    public Integer getMaxRowsToGroupBy() {
        return maxRowsToGroupBy;
    }

    public void setMaxRowsToGroupBy(Integer maxRowsToGroupBy) {
        this.maxRowsToGroupBy = maxRowsToGroupBy;
    }
}
