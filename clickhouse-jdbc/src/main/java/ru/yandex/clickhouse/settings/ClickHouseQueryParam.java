package ru.yandex.clickhouse.settings;


import java.sql.DriverPropertyInfo;
import java.util.Locale;
import java.util.Properties;

public enum ClickHouseQueryParam implements DriverPropertyCreator {

    ADD_HTTP_CORS_HEADER("add_http_cors_header", false, Boolean.class, "Write add http CORS header"),

    AGGREGATION_MEMORY_EFFICIENT_MERGE_THREADS("aggregation_memory_efficient_merge_threads", null, Long.class, ""),

    ALLOW_EXPERIMENTAL_BIGINT_TYPES("allow_experimental_bigint_types", null, Integer.class, "Enables or disables integer values exceeding the range that is supported by the int data type."),
    
    ALLOW_EXPERIMENTAL_MAP_TYPE("allow_experimental_map_type", null, Integer.class, "Enables or disables Map data type."),
    
    BACKGROUND_POOL_SIZE("background_pool_size", null, Long.class, ""),

    AUTHORIZATION("authorization", null, String.class, "Authorization header content for HTTP transport"),

    COMPILE("compile", false, Boolean.class, ""),

    COMPRESS("compress", true, Boolean.class, "whether to compress transferred data or not"),

    CONNECT_TIMEOUT("connect_timeout", null, Integer.class, ""),

    CONNECT_TIMEOUT_WITH_FAILOVER_MS("connect_timeout_with_failover_ms", null, Integer.class, ""),

    CONNECTIONS_WITH_FAILOVER_MAX_TRIES("connections_with_failover_max_tries", null, Long.class, ""),

    COUNT_DISTINCT_IMPLEMENTATION("count_distinct_implementation", null, String.class, "What aggregate function to use for implementation of count(DISTINCT ...)"),

    DATABASE("database", null, String.class, "database name used by default"),

    DECOMPRESS("decompress", false, Boolean.class, "whether to decompress transferred data or not"),

    DISTRIBUTED_AGGREGATION_MEMORY_EFFICIENT("distributed_aggregation_memory_efficient", false, Boolean.class, "Whether to optimize memory consumption for external aggregation"),

    DISTRIBUTED_CONNECTIONS_POOL_SIZE("distributed_connections_pool_size", null, Long.class, ""),

    DISTRIBUTED_DIRECTORY_MONITOR_SLEEP_TIME_MS("distributed_directory_monitor_sleep_time_ms", null, Long.class, ""),

    DISTRIBUTED_GROUP_BY_NO_MERGE("distributed_group_by_no_merge", false, Boolean.class, ""),

    DISTRIBUTED_PRODUCT_MODE("distributed_product_mode", null, String.class, ""),

    ENABLE_HTTP_COMPRESSION("enable_http_compression", false, Boolean.class, ""),
    /**
     * https://clickhouse.yandex/reference_en.html#Extreme values
     */
    EXTREMES("extremes", false, Boolean.class, "Whether to include extreme values."),

    FORCE_INDEX_BY_DATE("force_index_by_date", false, Boolean.class, ""),

    FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS("force_optimize_skip_unused_shards", 0, Integer.class, "Enables or disables query execution if optimize_skip_unused_shards is enabled and skipping of unused shards is not possible. If the skipping is not possible and the setting is enabled, an exception will be thrown."),

    FORCE_PRIMARY_KEY("force_primary_key", false, Boolean.class, ""),

    GLOBAL_SUBQUERIES_METHOD("global_subqueries_method", null, String.class, ""),

    GROUP_BY_TWO_LEVEL_THRESHOLD("group_by_two_level_threshold", null, Long.class, ""),

    GROUP_BY_TWO_LEVEL_THRESHOLD_BYTES("group_by_two_level_threshold_bytes", null, Long.class, ""),

    HTTP_NATIVE_COMPRESSION_DISABLE_CHECKSUMMING_ON_DECOMPRESS("http_native_compression_disable_checksumming_on_decompress", null, Boolean.class, "Whether to disable checksum check on decompress"),

    HTTP_ZLIB_COMPRESSION_LEVEL("http_zlib_compression_level", null, Long.class, ""),

    INPUT_FORMAT_SKIP_UNKNOWN_FIELDS("input_format_skip_unknown_fields", false, Boolean.class, "Skip columns with unknown names from input data (it works for JSONEachRow and TSKV formats)."),

    INPUT_FORMAT_VALUES_INTERPRET_EXPRESSIONS("input_format_values_interpret_expressions", true, Boolean.class,
            "For Values format: if field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression."),

    INSERT_DEDUPLICATE("insert_deduplicate", null, Boolean.class, "For INSERT queries in the replicated table, specifies that deduplication of insertings blocks should be preformed"),

    INSERT_DISTRIBUTED_SYNC("insert_distributed_sync", null, Boolean.class, "If setting is enabled, insert query into distributed waits until data will be sent to all nodes in cluster."),

    ANY_JOIN_DISTINCT_RIGHT_TABLE_KEYS("any_join_distinct_right_table_keys", null, Boolean.class, "Setting enables old behaviour of ANY INNER|RIGHT|FULL JOIN which are disabled by default from version 19.14.3.3"),

    INSERT_QUORUM("insert_quorum", null, Long.class, ""),

    INSERT_QUORUM_TIMEOUT("insert_quorum_timeout", null, Long.class, ""),

    INTERACTIVE_DELAY("interactive_delay", null, Long.class, ""),

    JOIN_ALGORITHM("join_algorithm", null, String.class, ""),

    LOAD_BALANCING("load_balancing", null, String.class, ""),

    LOG_QUERIES("log_queries", false, Boolean.class, ""),

    LOG_QUERIES_CUT_TO_LENGTH("log_queries_cut_to_length", null, Long.class, ""),

    MARK_CACHE_MIN_LIFETIME("mark_cache_min_lifetime", null, Long.class, ""),
    /**
     * https://clickhouse.yandex/reference_en.html#max_block_size
     */
    MAX_BLOCK_SIZE("max_block_size", null, Integer.class, "Recommendation for what size of block (in number of rows) to load from tables"),

    MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY("max_bytes_before_external_group_by", null, Long.class, "Threshold to use external group by"),

    MAX_BYTES_BEFORE_EXTERNAL_SORT("max_bytes_before_external_sort", null, Long.class, "Threshold to use external sort"),

    MAX_COMPRESS_BLOCK_SIZE("max_compress_block_size", null, Long.class, ""),

    MAX_CONCURRENT_QUERIES_FOR_USER("max_concurrent_queries_for_user", null, Long.class, ""),

    MAX_DISTRIBUTED_CONNECTIONS("max_distributed_connections", null, Long.class, ""),

    MAX_DISTRIBUTED_PROCESSING_THREADS("max_distributed_processing_threads", null, Long.class, ""),
    /**
     * https://clickhouse.yandex/reference_en.html#max_execution_time
     */
    MAX_EXECUTION_TIME("max_execution_time", null, Integer.class, "Maximum query execution time in seconds."),

    MAX_INSERT_BLOCK_SIZE("max_insert_block_size", null, Long.class, ""),

    /**
     * @see <a href="https://clickhouse.yandex/reference_en.html#max_memory_usage">max_memory_usage</a>
     */
    MAX_MEMORY_USAGE("max_memory_usage", null, Long.class, "The maximum amount of memory consumption when running a query on a single server."),

    /**
     * @see <a href="https://clickhouse.yandex/docs/en/operations/settings/query_complexity/#max-memory-usage-for-user">max_memory_usage_for_user</a>
     */
    MAX_MEMORY_USAGE_FOR_USER("max_memory_usage_for_user", null, Long.class, "The maximum amount of RAM to use for running a user's queries on a single server."),

    /**
     * @see <a href="https://clickhouse.yandex/docs/en/operations/settings/query_complexity/#max-memory-usage-for-all-queries">max_memory_usage_for_all_queries</a>
     */
    MAX_MEMORY_USAGE_FOR_ALL_QUERIES("max_memory_usage_for_all_queries", null, Long.class, "The maximum amount of RAM to use for running all queries on a single server."),

    //dbms/include/DB/Interpreters/Settings.h
    MAX_PARALLEL_REPLICAS("max_parallel_replicas", null, Integer.class, "Max shard replica count."),

    MAX_PARTITIONS_PER_INSERT_BLOCK("max_partitions_per_insert_block", null, Integer.class, "If inserted block contains larger number of partitions, an exception is thrown. Set it to 0 if you want to remove the limit (not recommended)."),

    MAX_READ_BUFFER_SIZE("max_read_buffer_size", null, Long.class, ""),

    MAX_RESULT_ROWS("max_result_rows", null, Integer.class, "Limit on the number of rows in the result. Also checked for subqueries, and on remote servers when running parts of a distributed query."),
    /**
     * https://clickhouse.yandex/reference_en.html#max_rows_to_group_by
     */
    MAX_ROWS_TO_GROUP_BY("max_rows_to_group_by", null, Integer.class,
            "Maximum number of unique keys received from aggregation. This setting lets you limit memory consumption when aggregating."),

    MAX_STREAMS_TO_MAX_THREADS_RATIO("max_streams_to_max_threads_ratio", null, Double.class, ""),
    /**
     * https://clickhouse.yandex/reference_en.html#max_threads
     */
    MAX_THREADS("max_threads", null, Integer.class, "The maximum number of query processing threads"),

    MAX_QUERY_SIZE("max_query_size", null, Long.class, "Maximum size of query"),

    MAX_AST_ELEMENTS("max_ast_elements", null, Long.class, "Maximum number of elements in a query syntactic tree"),

    MEMORY_TRACKER_FAULT_PROBABILITY("memory_tracker_fault_probability", null, Double.class, ""),

    MERGE_TREE_COARSE_INDEX_GRANULARITY("merge_tree_coarse_index_granularity", null, Long.class, ""),

    MERGE_TREE_MAX_ROWS_TO_USE_CACHE("merge_tree_max_rows_to_use_cache", null, Long.class, ""),

    MERGE_TREE_MIN_ROWS_FOR_CONCURRENT_READ("merge_tree_min_rows_for_concurrent_read", null, Long.class, ""),

    MERGE_TREE_MIN_ROWS_FOR_SEEK("merge_tree_min_rows_for_seek", null, Long.class, ""),

    MERGE_TREE_UNIFORM_READ_DISTRIBUTION("merge_tree_uniform_read_distribution", true, Boolean.class, ""),

    MIN_BYTES_TO_USE_DIRECT_IO("min_bytes_to_use_direct_io", null, Long.class, ""),

    MIN_COMPRESS_BLOCK_SIZE("min_compress_block_size", null, Long.class, ""),

    MIN_COUNT_TO_COMPILE("min_count_to_compile", null, Long.class, ""),

    MIN_INSERT_BLOCK_SIZE_BYTES("min_insert_block_size_bytes", null, Long.class, "Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enoug"),

    MIN_INSERT_BLOCK_SIZE_ROWS("min_insert_block_size_rows", null, Long.class, "Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough."),

    NETWORK_COMPRESSION_METHOD("network_compression_method", null, String.class, ""),

    OPTIMIZE_MIN_EQUALITY_DISJUNCTION_CHAIN_LENGTH("optimize_min_equality_disjunction_chain_length", null, Long.class, ""),

    OPTIMIZE_MOVE_TO_PREWHERE("optimize_move_to_prewhere", true, Boolean.class, ""),

    OPTIMIZE_SKIP_UNUSED_SHARDS("optimize_skip_unused_shards", 0, Integer.class, "Enables or disables skipping of unused shards for SELECT queries that have sharding key condition in WHERE/PREWHERE (assuming that the data is distributed by sharding key, otherwise does nothing)."),

    OUTPUT_FORMAT_JSON_QUOTE_64BIT_INTEGERS("output_format_json_quote_64bit_integers", true, Boolean.class, "Controls quoting of 64-bit integers in JSON output format."),

    OUTPUT_FORMAT_PRETTY_MAX_ROWS("output_format_pretty_max_rows", null, Long.class, "Rows limit for Pretty formats."),

    OUTPUT_FORMAT_WRITE_STATISTICS("output_format_write_statistics", true, Boolean.class, "Write statistics about read rows, bytes, time elapsed in suitable output formats"),

    PARALLEL_REPLICAS_COUNT("parallel_replicas_count", null, Long.class, ""),

    PARALLEL_REPLICA_OFFSET("parallel_replica_offset", null, Long.class, ""),

    PASSWORD("password", null, String.class, "user password, by default null"),

    POLL_INTERVAL("poll_interval", null, Long.class, ""),

    PRIORITY("priority", null, Integer.class, "The lower the value the bigger the priority."),
    /**
     * https://clickhouse.yandex/reference_en.html#Settings profiles
     */
    PROFILE("profile", null, String.class, "Settings profile: a collection of settings grouped under the same name"),

    RECEIVE_TIMEOUT("receive_timeout", null, Integer.class, ""),

    READ_BACKOFF_MAX_THROUGHPUT("read_backoff_max_throughput", null, Long.class, ""),

    READ_BACKOFF_MIN_EVENTS("read_backoff_min_events", null, Long.class, ""),

    READ_BACKOFF_MIN_INTERVAL_BETWEEN_EVENTS_MS("read_backoff_min_interval_between_events_ms", null, Long.class, ""),

    READ_BACKOFF_MIN_LATENCY_MS("read_backoff_min_latency_ms", null, Long.class, ""),

    REPLACE_RUNNING_QUERY("replace_running_query", false, Boolean.class, ""),

    REPLICATION_ALTER_COLUMNS_TIMEOUT("replication_alter_columns_timeout", null, Long.class, ""),

    REPLICATION_ALTER_PARTITIONS_SYNC("replication_alter_partitions_sync", null, Long.class, ""),

    RESHARDING_BARRIER_TIMEOUT("resharding_barrier_timeout", null, Long.class, ""),

    RESULT_OVERFLOW_MODE("result_overflow_mode", null, String.class, "What to do if the volume of the result exceeds one of the limits: 'throw' or 'break'. By default, throw. Using 'break' is similar to using LIMIT."),

    SELECT_SEQUENTIAL_CONSISTENCY("select_sequential_consistency", null, Long.class, ""),

    SEND_PROGRESS_IN_HTTP_HEADERS("send_progress_in_http_headers", null, Boolean.class, "Allow to populate summary in ClickHouseStatement with read/written rows/bytes"),

    SEND_TIMEOUT("send_timeout", null, Integer.class, ""),

    SESSION_CHECK("session_check", false, Boolean.class, ""),

    SESSION_ID("session_id", null, String.class, ""),

    SESSION_TIMEOUT("session_timeout", null, Long.class, ""),

    SKIP_UNAVAILABLE_SHARDS("skip_unavailable_shards", false, Boolean.class, ""),

    STRICT_INSERT_DEFAULTS("strict_insert_defaults", false, Boolean.class, ""),

    TABLE_FUNCTION_REMOTE_MAX_ADDRESSES("table_function_remote_max_addresses", null, Long.class, ""),

    TOTALS_AUTO_THRESHOLD("totals_auto_threshold", null, Double.class, ""),
    /**
     * https://clickhouse.yandex/reference_en.html#WITH TOTALS modifier
     */
    TOTALS_MODE("totals_mode", null, String.class, "How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = 'any' are present."),

    QUERY_ID("query_id", null, String.class, ""),

    QUEUE_MAX_WAIT_MS("queue_max_wait_ms", null, Integer.class, ""),

    QUOTA_KEY("quota_key", null, String.class, "quota is calculated for each quota_key value. For example here may be some user name."),

    @Deprecated
    use_client_time_zone("use_client_time_zone", false, Boolean.class, ""),

    USE_UNCOMPRESSED_CACHE("use_uncompressed_cache", true, Boolean.class, "Whether to use the cache of uncompressed blocks."),

    USER("user", null, String.class, "user name, by default - default"),

    PREFERRED_BLOCK_SIZE_BYTES("preferred_block_size_bytes", null, Long.class, "Adaptively estimates number of required rows in a block."),

    ENABLE_OPTIMIZE_PREDICATE_EXPRESSION("enable_optimize_predicate_expression", null, Boolean.class, "See Clickhouse server description for this parameter. Default value is null so that server setting is taken."),

    WAIT_END_OF_QUERY("wait_end_of_query", null, Boolean.class, "Buffer the response server-side before sending to client. Useful when using SEND_PROGRESS_IN_HTTP_HEADERS to get accurate stats."),

    INPUT_FORMAT_ALLOW_ERRORS_NUM("input_format_allow_errors_num", null, Integer.class, "Maximum absolute amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue."),

    INPUT_FORMAT_ALLOW_ERRORS_RATIO("input_format_allow_errors_ratio", null, Double.class, "Maximum relative amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.")
    ;

    private final String key;
    private final Object defaultValue;
    private final Class<?> clazz;
    private final String description;

    <T> ClickHouseQueryParam(String key, T defaultValue, Class<T> clazz, String description) {
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

    public Class<?> getClazz() {
        return clazz;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    @Override
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
