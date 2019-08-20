package ru.yandex.clickhouse.settings;


import java.sql.DriverPropertyInfo;
import java.util.Properties;

public enum ClickHouseConnectionSettings implements DriverPropertyCreator {

    ASYNC("async", false, ""),
    BUFFER_SIZE("buffer_size", 65536, ""),
    APACHE_BUFFER_SIZE("apache_buffer_size", 65536, ""),
    SOCKET_TIMEOUT("socket_timeout", 30000, ""),
    CONNECTION_TIMEOUT("connection_timeout", 10 * 1000, "connection timeout in milliseconds"),
    SSL("ssl", false, "enable SSL/TLS for the connection"),
    SSL_ROOT_CERTIFICATE("sslrootcert", "", "SSL/TLS root certificate"),
    SSL_MODE("sslmode", "strict", "verify or not certificate: none (don't verify), strict (verify)"),
    USE_PATH_AS_DB("use_path_as_db", true, "whether URL path should be treated as database name"),
    PATH("path", "/", "URL path"),
    CHECK_FOR_REDIRECTS("check_for_redirects", false, "whether we should check for 307 redirect using GET before sending POST to given URL"),
    MAX_REDIRECTS("max_redirects", 5, "number of redirect checks before using last URL"),

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
    USE_OBJECTS_IN_ARRAYS("use_objects_in_arrays", false, "Whether Object[] should be used instead primitive arrays."),
    MAX_COMPRESS_BUFFER_SIZE("maxCompressBufferSize", 1024*1024, ""),

    USE_SERVER_TIME_ZONE("use_server_time_zone", true, "Whether to use timezone from server. On connection init select timezone() will be executed"),
    USE_TIME_ZONE("use_time_zone", "", "Which time zone to use"),
    USE_SERVER_TIME_ZONE_FOR_DATES("use_server_time_zone_for_dates", false,
            "Whether to use timezone from server on Date parsing in getDate(). " +
                    "If false, Date returned is a wrapper of a timestamp at start of the day in client timezone. " +
                    "If true - at start of the day in server or use_timezone timezone.")
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
