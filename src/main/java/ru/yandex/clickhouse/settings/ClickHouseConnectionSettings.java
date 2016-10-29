package ru.yandex.clickhouse.settings;


public enum ClickHouseConnectionSettings {

    ASYNC("async", false, "FIXME"),
    BUFFER_SIZE("buffer_size", 65536, "FIXME"),
    APACHE_BUFFER_SIZE("apache_buffer_size", 65536, "FIXME"),
    SOCKET_TIMEOUT("socket_timeout", 30000, "FIXME"),
    CONNECTION_TIMEOUT("connection_timeout", 50, "FIXME"),

    /*
    * this is a timeout for data transfer
    * socketTimeout + dataTransferTimeout is sent to ClickHouse as max_execution_time
    * ClickHouse rejects request execution if its time exceeds max_execution_time
    * */
    DATA_TRANSFER_TIMEOUT( "dataTransferTimeout", 10000, "FIXME"),


    KEEP_ALIVE_TIMEOUT("keepAliveTimeout", 30 * 1000, "FIXME"),

    /**
     * for ConnectionManager
     */
    TIME_TO_LIVE_MILLIS("timeToLiveMillis", 60*1000, "FIXME"),
    DEFAULT_MAX_PER_ROUTE("defaultMaxPerRoute", 500, "FIXME"),
    MAX_TOTAL("maxTotal", 10000, "FIXME");

    private final String key;
    private final Object defaultValue;
    private final String description;
    private final Class clazz;

    ClickHouseConnectionSettings(String key, Object defaultValue, String description) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = defaultValue.getClass();
        this.description = description;
    }

    public String getKey() {
        return key;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public Class getClazz() {
        return clazz;
    }

    public String getDescription() {
        return description;
    }
}
