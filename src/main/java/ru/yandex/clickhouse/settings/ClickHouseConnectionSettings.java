package ru.yandex.clickhouse.settings;


import java.sql.DriverPropertyInfo;
import java.util.Properties;

public enum ClickHouseConnectionSettings implements DriverPropertyCreator {

    ASYNC("async", false, ""),
    BUFFER_SIZE("buffer_size", 65536, ""),
    APACHE_BUFFER_SIZE("apache_buffer_size", 65536, ""),
    SOCKET_TIMEOUT("socket_timeout", 30000, ""),
    CONNECTION_TIMEOUT("connection_timeout", 50, ""),

    /*
    *
    * */
    DATA_TRANSFER_TIMEOUT( "dataTransferTimeout", 10000, "Timeout for data transfer. "
            + " socketTimeout + dataTransferTimeout is sent to ClickHouse as max_execution_time. "
            + " ClickHouse rejects request execution if its time exceeds max_execution_time"),


    KEEP_ALIVE_TIMEOUT("keepAliveTimeout", 30 * 1000, ""),

    /**
     * for ConnectionManager
     */
    TIME_TO_LIVE_MILLIS("timeToLiveMillis", 60 * 1000, ""),
    DEFAULT_MAX_PER_ROUTE("defaultMaxPerRoute", 500, ""),
    MAX_TOTAL("maxTotal", 10000, ""),

    /**
     * additional
     */
    MAX_COMPRESS_BUFFER_SIZE("maxCompressBufferSize", 1024*1024, "")
    ;

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

    public DriverPropertyInfo createDriverPropertyInfo(Properties properties) {
        DriverPropertyInfo propertyInfo = new DriverPropertyInfo(key, driverPropertyValue(properties));
        propertyInfo.required = false;
        propertyInfo.description = description;
        propertyInfo.choices = driverPropertyInfoChoices();
        return propertyInfo;
    }

    private String[] driverPropertyInfoChoices() {
        return clazz == Boolean.class || clazz == Boolean.TYPE ? new String[]{"true", "false"} : null;
    }

    private String driverPropertyValue(Properties properties) {
        String value = properties.getProperty(key);
        if (value == null) {
            value = defaultValue == null ? null : defaultValue.toString();
        }
        return value;
    }
}
