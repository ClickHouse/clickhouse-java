package ru.yandex.metrika.clickhouse.copypaste;

/**
 * @author serebrserg
 * @since 24.03.16
 */
class CHConnectionSettings {
    static final String PROFILE_SETTING = "profile";
    static final String PROFILE_SETTING_DEFAULT = null;

    static final String COMPRESS_SETTING = "compress";
    static final boolean COMPRESS_SETTING_DEFAULT = true;

    static final String ASYNC_SETTING = "async";
    static final boolean ASYNC_SETTING_DEFAULT = false;

    static final String MAX_THREADS_SETTING = "maxThreads";
    static final Integer MAX_THREADS_SETTING_DEFAULT = null;

    static final String MAX_BLOCK_SIZE_SETTING = "maxBlockSize";
    static final Integer MAX_BLOCK_SIZE_SETTING_DEFAULT = null;

    static final String BUFFER_SIZE_SETTING = "bufferSize";
    static final int BUFFER_SIZE_SETTING_DEFAULT = 65536;

    static final String APACHE_BUFFER_SIZE_SETTING = "apacheBufferSize";
    static final int APACHE_BUFFER_SIZE_SETTING_DEFAULT = 65536;

    static final String SOCKET_TIMEOUT_SETTING = "socketTimeout";
    static final int SOCKET_TIMEOUT_SETTING_DEFAULT = 30000;

    static final String CONNECTION_TIMEOUT_SETTING = "connectionTimeout";
    static final int CONNECTION_TIMEOUT_SETTING_DEFAULT = 50;

    static final String USER_SETTING = "user";
    static final String USER_SETTING_DEFAULT = null;

    /*
    * это таймаут на передачу данных.
    * Число socketTimeout + dataTransferTimeout отправляется в clickhouse в параметре max_execution_time
    * После чего кликхаус сам останавливает запрос если время его выполнения превышает max_execution_time
    * */
    static final String DATA_TRANSFER_TIMEOUT_SETTING = "dataTransferTimeout";
    static final int DATA_TRANSFER_TIMEOUT_SETTING_DEFAULT = 10000;

    static final String KEEP_ALIVE_TIMEOUT_SETTING = "keepAliveTimeout";
    static final int KEEP_ALIVE_TIMEOUT_SETTING_DEFAULT = 30 * 100;

    /**
     * Для ConnectionManager'а
     */
    static final String TIME_TO_LIVE_MILLIS_SETTING = "timeToLiveMillis";
    static final int TIME_TO_LIVE_MILLIS_SETTING_DEFAULT = 60*1000;

    static final String DEFAULT_MAX_PER_ROUTE_SETTING = "defaultMaxPerRoute";
    static final int DEFAULT_MAX_PER_ROUTE_SETTING_DEFAULT = 500;

    static final String MAX_TOTAL_SETTING = "maxTotal";
    static final int MAX_TOTAL_SETTING_DEFAULT = 1000;

}
