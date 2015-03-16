package ru.yandex.metrika.clickhouse.copypaste;

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
    private boolean compress = true;
    // asynchronous=0&max_threads=1
    private boolean async;
    private Integer maxThreads;
    private Integer maxBlockSize;

    private int bufferSize = 65536;
    private int apacheBufferSize = 65536;

    //настройки для демонов
    private int socketTimeout = 30000;
    private int connectionTimeout = 50;

    //METR-9568: параметр user для определения профиля настроек(?).
    private String user = null;

    /*
    * это таймаут на передачу данных.
    * Число socketTimeout + dataTransferTimeout отправляется в clickhouse в параметре max_execution_time
    * После чего кликхаус сам останавливает запрос если время его выполнения превышает max_execution_time
    * */
    private int dataTransferTimeout = 10000;
    private int keepAliveTimeout = 30 * 1000;

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
}
