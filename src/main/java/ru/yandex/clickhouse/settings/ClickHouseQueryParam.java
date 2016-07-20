package ru.yandex.clickhouse.settings;


public enum ClickHouseQueryParam {
    MAX_PARALLEL_REPLICAS("max_parallel_replicas", null, Integer.class),
    /**
     * How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = 'any' are present.
     * https://clickhouse.yandex/reference_en.html#WITH TOTALS modifier
     */
    TOTALS_MODE("totals_mode", null, String.class),
    /**
     * quota is calculated for each quota_key value.
     * For example here may be some user name.
     */
    QUOTA_KEY("quota_key", null, String.class),
    /**
     * The lower the value the bigger the priority.
     */
    PRIORITY("priority", null, Integer.class),
    /**
     * database name used by default
     */
    DATABASE("database", null, String.class),
    /**
     *  whether to compress transfered data or not
     */
    COMPRESS("compress", true, Boolean.class),
    /**
     * Whether to include extreme values.
     * https://clickhouse.yandex/reference_en.html#Extreme values
     */
    EXTREMES("extremes", false, String.class),
    /**
     * The maximum number of query processing threads
     * https://clickhouse.yandex/reference_en.html#max_threads
     */
    MAX_THREADS("max_threads", null, Integer.class),
    /**
     * Maximum query execution time in seconds.
     * https://clickhouse.yandex/reference_en.html#max_execution_time
     */
    MAX_EXECUTION_TIME("max_execution_time", null, Integer.class),
    /**
     * https://clickhouse.yandex/reference_en.html#max_block_size
     */
    MAX_BLOCK_SIZE("max_block_size", null, Integer.class),

    /**
     * Maximum number of unique keys received from aggregation. This setting lets you limit memory consumption when aggregating.
     * https://clickhouse.yandex/reference_en.html#max_rows_to_group_by
     */
    MAX_ROWS_TO_GROUP_BY("max_rows_to_group_by", null, Integer.class),

    /**
     * https://clickhouse.yandex/reference_en.html#Settings profiles
     */
    PROFILE("profile", null, String.class),
    /**
     *  user name, by default - default
     */
    USER("user", null, String.class),

    /**
     * user password, by default null
     */
    PASSWORD("password", null, String.class);

    private final String key;
    private final Object defaultValue;
    private final Class clazz;

    ClickHouseQueryParam(String key, Object defaultValue, Class clazz) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = clazz;
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

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
