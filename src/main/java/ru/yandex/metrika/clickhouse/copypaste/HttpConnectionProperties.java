package ru.yandex.metrika.clickhouse.copypaste;

import java.util.Properties;

import static ru.yandex.metrika.clickhouse.copypaste.CHConnectionSettings.*;
import static ru.yandex.metrika.clickhouse.copypaste.CHQueryParam.*;
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
    private String profile;
    private boolean compress;
    // asynchronous=0&max_threads=1
    private boolean async;
    private Integer maxThreads;
    private Integer maxBlockSize;

    private int bufferSize;
    private int apacheBufferSize;

    //настройки для демонов
    private int socketTimeout;
    private int connectionTimeout;

    //METR-9568: параметр user для определения профиля настроек(?).
    private String user;

    /*
    * это таймаут на передачу данных.
    * Число socketTimeout + dataTransferTimeout отправляется в clickhouse в параметре max_execution_time
    * После чего кликхаус сам останавливает запрос если время его выполнения превышает max_execution_time
    * */
    private int dataTransferTimeout;
    private int keepAliveTimeout;

    /**
     * Для ConnectionManager'а
     */
    private int timeToLiveMillis;
    private int defaultMaxPerRoute;
    private int maxTotal;

    public HttpConnectionProperties() {
        this(new Properties());
    }

    public HttpConnectionProperties(Properties info) {
        this.profile = getSetting(info, PROFILE);
        this.compress = getSetting(info, COMPRESS);
        this.async = getSetting(info, ASYNC);
        this.maxThreads = getSetting(info, MAX_THREADS);
        this.maxBlockSize = getSetting(info, MAX_BLOCK_SIZE);

        this.bufferSize = getSetting(info, BUFFER_SIZE);
        this.apacheBufferSize = getSetting(info, APACHE_BUFFER_SIZE);

        this.socketTimeout = getSetting(info, SOCKET_TIMEOUT);
        this.connectionTimeout = getSetting(info, CONNECTION_TIMEOUT);

        this.user = getSetting(info, USER);

        this.dataTransferTimeout = getSetting(info, DATA_TRANSFER_TIMEOUT);
        this.keepAliveTimeout = getSetting(info, KEEP_ALIVE_TIMEOUT);

        this.timeToLiveMillis = getSetting(info, TIME_TO_LIVE_MILLIS);
        this.defaultMaxPerRoute = getSetting(info, DEFAULT_MAX_PER_ROUTE);
        this.maxTotal = getSetting(info, MAX_TOTAL);
    }


    private <T> T getSetting(Properties info, CHQueryParam param){
        return getSetting(info, param.getKey(), param.getDefaultValue(), param.getClazz());
    }

    private <T> T getSetting(Properties info, CHConnectionSettings settings){
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

}
