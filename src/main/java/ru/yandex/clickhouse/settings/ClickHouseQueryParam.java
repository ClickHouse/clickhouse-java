package ru.yandex.clickhouse.settings;


import java.sql.DriverPropertyInfo;
import java.util.Properties;

public enum ClickHouseQueryParam implements DriverPropertyInfoAware{
    //dbms/include/DB/Interpreters/Settings.h
    MAX_PARALLEL_REPLICAS("max_parallel_replicas", null, Integer.class, "max shard replica count "),
    /**
     * https://clickhouse.yandex/reference_en.html#WITH TOTALS modifier
     */
    TOTALS_MODE("totals_mode", null, String.class, "How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = 'any' are present."),
    QUOTA_KEY("quota_key", null, String.class, "quota is calculated for each quota_key value. For example here may be some user name."),
    PRIORITY("priority", null, Integer.class, "The lower the value the bigger the priority."),
    DATABASE("database", null, String.class, "database name used by default"),
    COMPRESS("compress", true, Boolean.class, "whether to compress transferred data or not"),
    /**
     * https://clickhouse.yandex/reference_en.html#Extreme values
     */
    EXTREMES("extremes", false, Boolean.class, "Whether to include extreme values."),
    /**
     * https://clickhouse.yandex/reference_en.html#max_threads
     */
    MAX_THREADS("max_threads", null, Integer.class, "The maximum number of query processing threads"),
    /**
     * https://clickhouse.yandex/reference_en.html#max_execution_time
     */
    MAX_EXECUTION_TIME("max_execution_time", null, Integer.class, "Maximum query execution time in seconds."),
    /**
     * https://clickhouse.yandex/reference_en.html#max_block_size
     */
    MAX_BLOCK_SIZE("max_block_size", null, Integer.class, "Recommendation for what size of block (in number of rows) to load from tables"),

    /**
     * https://clickhouse.yandex/reference_en.html#max_rows_to_group_by
     */
    MAX_ROWS_TO_GROUP_BY("max_rows_to_group_by", null, Integer.class,
            "Maximum number of unique keys received from aggregation. This setting lets you limit memory consumption when aggregating."),

    /**
     * https://clickhouse.yandex/reference_en.html#Settings profiles
     */
    PROFILE("profile", null, String.class, "Settings profile: a collection of settings grouped under the same name"),
    USER("user", null, String.class, "user name, by default - default"),
    PASSWORD("password", null, String.class, "user password, by default null");

    private final String key;
    private final Object defaultValue;
    private final Class clazz;
    private final String description;

    ClickHouseQueryParam(String key, Object defaultValue, Class clazz, String description) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = clazz;
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

    public String getDescription(){
        return description;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }

    public DriverPropertyInfo toDriverPropertyInfo(Properties properties) {
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
