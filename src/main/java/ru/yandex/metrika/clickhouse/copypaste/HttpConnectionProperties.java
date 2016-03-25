package ru.yandex.metrika.clickhouse.copypaste;

import java.util.Properties;

import static ru.yandex.metrika.clickhouse.copypaste.CHConnectionSettings.*;
/**
 * User: hamilkar
 * Date: 10/17/13
 * Time: 2:48 PM
 */
public class HttpConnectionProperties {

    // Настройки кликхауса

    /**
     * profile=web&sign_rewrite=0
     * На стороне clickhouse сделаны ограничения на запросы.
     * https://svn.yandex.ru/websvn/wsvn/conv/trunk/metrica/src/dbms/src/Server/config.conf
     */
    private String profile = PROFILE_SETTING_DEFAULT;
    private boolean compress = COMPRESS_SETTING_DEFAULT;
    // asynchronous=0&max_threads=1
    private boolean async = ASYNC_SETTING_DEFAULT;
    private Integer maxThreads = MAX_THREADS_SETTING_DEFAULT;
    private Integer maxBlockSize = MAX_BLOCK_SIZE_SETTING_DEFAULT;

    private int bufferSize = BUFFER_SIZE_SETTING_DEFAULT;
    private int apacheBufferSize = APACHE_BUFFER_SIZE_SETTING_DEFAULT;

    //настройки для демонов
    private int socketTimeout = SOCKET_TIMEOUT_SETTING_DEFAULT;
    private int connectionTimeout = CONNECTION_TIMEOUT_SETTING_DEFAULT;

    //METR-9568: параметр user для определения профиля настроек(?).
    private String user = USER_SETTING_DEFAULT;

    /*
    * это таймаут на передачу данных.
    * Число socketTimeout + dataTransferTimeout отправляется в clickhouse в параметре max_execution_time
    * После чего кликхаус сам останавливает запрос если время его выполнения превышает max_execution_time
    * */
    private int dataTransferTimeout = DATA_TRANSFER_TIMEOUT_SETTING_DEFAULT;
    private int keepAliveTimeout = KEEP_ALIVE_TIMEOUT_SETTING_DEFAULT;

    /**
     * Для ConnectionManager'а
     */
    private int timeToLiveMillis = TIME_TO_LIVE_MILLIS_SETTING_DEFAULT;
    private int defaultMaxPerRoute = DEFAULT_MAX_PER_ROUTE_SETTING_DEFAULT;
    private int maxTotal = MAX_TOTAL_SETTING_DEFAULT;

    public HttpConnectionProperties() {
    }

    public HttpConnectionProperties(Properties info) {
        this.profile = getSetting(info, PROFILE_SETTING, PROFILE_SETTING_DEFAULT);
        this.compress = getSetting(info, COMPRESS_SETTING, COMPRESS_SETTING_DEFAULT);
        this.async = getSetting(info, ASYNC_SETTING, ASYNC_SETTING_DEFAULT);
        this.maxThreads = getSetting(info, MAX_THREADS_SETTING, MAX_THREADS_SETTING_DEFAULT);
        this.maxBlockSize = getSetting(info, MAX_BLOCK_SIZE_SETTING, MAX_BLOCK_SIZE_SETTING_DEFAULT);

        this.bufferSize = getSetting(info, BUFFER_SIZE_SETTING, BUFFER_SIZE_SETTING_DEFAULT);
        this.apacheBufferSize = getSetting(info, APACHE_BUFFER_SIZE_SETTING,APACHE_BUFFER_SIZE_SETTING_DEFAULT);

        this.socketTimeout = getSetting(info, SOCKET_TIMEOUT_SETTING, SOCKET_TIMEOUT_SETTING_DEFAULT);
        this.connectionTimeout = getSetting(info, CONNECTION_TIMEOUT_SETTING, CONNECTION_TIMEOUT_SETTING_DEFAULT);

        this.user = getSetting(info, USER_SETTING, USER_SETTING_DEFAULT);

        this.dataTransferTimeout = getSetting(info, DATA_TRANSFER_TIMEOUT_SETTING, DATA_TRANSFER_TIMEOUT_SETTING_DEFAULT);
        this.keepAliveTimeout = getSetting(info, KEEP_ALIVE_TIMEOUT_SETTING, KEEP_ALIVE_TIMEOUT_SETTING_DEFAULT);

        this.timeToLiveMillis = getSetting(info, TIME_TO_LIVE_MILLIS_SETTING, TIME_TO_LIVE_MILLIS_SETTING_DEFAULT);
        this.defaultMaxPerRoute = getSetting(info, DEFAULT_MAX_PER_ROUTE_SETTING, DEFAULT_MAX_PER_ROUTE_SETTING_DEFAULT);
        this.maxTotal = getSetting(info, MAX_TOTAL_SETTING, MAX_TOTAL_SETTING_DEFAULT);
    }

    @SuppressWarnings("unchecked")
    private <T> T getSetting(Properties info, String key, T defaultValue){
        Object val = info.get(key);
        if (val == null)
            return defaultValue;
        if ((defaultValue.getClass() == int.class || defaultValue.getClass() == Integer.class) && val instanceof String) {
            return (T) defaultValue.getClass().cast(Integer.valueOf((String) val));
        }
        if ((defaultValue.getClass() == long.class || defaultValue.getClass() == Long.class) && val instanceof String) {
            return (T) defaultValue.getClass().cast(Long.valueOf((String) val));
        }
        if ((defaultValue.getClass() == boolean.class || defaultValue.getClass() == Boolean.class) && val instanceof String) {
            return (T) defaultValue.getClass().cast(Boolean.valueOf((String) val));
        }
        return (T) defaultValue.getClass().cast(val);
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

}
