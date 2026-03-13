package com.clickhouse.client.cli;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Simple CLI tool that mimics clickhouse-client.
 * Executes a SQL query against a ClickHouse server and prints results in TSV format.
 *
 * <p>Supports two backend implementations selected via the {@code CLICKHOUSE_CLIENT_CLI_IMPL}
 * environment variable:
 * <ul>
 *   <li>{@code client} (default) &ndash; uses the ClickHouse client-v2 API</li>
 *   <li>{@code jdbc} &ndash; uses the ClickHouse JDBC driver</li>
 * </ul>
 *
 * Usage:
 *   java -jar clickhouse-client-cli.jar [options]
 *
 * Options:
 *   --host, -h       Server host (default: localhost)
 *   --port           HTTP port (default: 8123)
 *   --user, -u       Username (default: default)
 *   --password       Password (default: empty)
 *   --database, -d   Database (default: default)
 *   --query, -q      SQL query to execute
 *   --log_comment    Comment for query_log records
 *   --send_logs_level Server log level to send with result
 *   --max_insert_threads Max insert threads setting
 *   --multiquery     Execute multiple ';'-separated queries
 *   --multiline, -n  (ignored, accepted for compatibility)
 *   --help           Print usage
 *
 * If --query is not specified, the query is read from stdin.
 */
public class Main {

    private static final long QUERY_TIMEOUT_SECONDS = 300;
    private static final String LOG_PATH_ENV = "CLICKHOUSE_CLIENT_CLI_LOG";
    private static final String IMPL_ENV = "CLICKHOUSE_CLIENT_CLI_IMPL";
    private static final String IMPL_CLIENT = "client";
    private static final String IMPL_JDBC = "jdbc";
    private static final Path DEFAULT_LOG_PATH = Paths.get("/tmp/clickhouse-client-cli.log");
    private static final Path FALLBACK_LOG_PATH = Paths.get("clickhouse-client-cli.log");
    private static final CSVFormat CLICKHOUSE_TSV_FORMAT = CSVFormat.TDF.builder()
            .setQuote(null)
            .setEscape('\\')
            .setRecordSeparator('\n')
            .setNullString("\\N")
            .build();
    private static final String USAGE_HEADER = System.lineSeparator()
            + "Known server settings are forwarded to ClickHouse."
            + System.lineSeparator()
            + "Client-only and unknown settings are accepted but not sent to server."
            + System.lineSeparator()
            + "If --query is not specified, the query is read from stdin."
            + System.lineSeparator()
            + System.lineSeparator()
            + "Environment variables:"
            + System.lineSeparator()
            + "  CLICKHOUSE_CLIENT_CLI_IMPL   Backend implementation: 'client' (default) or 'jdbc'"
            + System.lineSeparator()
            + "  CLICKHOUSE_CLIENT_CLI_LOG    Path to log file for troubleshooting";
    private static final Set<String> CLIENT_ONLY_SETTINGS = createClientOnlySettings();
    private static final Set<String> SERVER_SETTINGS = createServerSettings();

    public static void main(String[] args) {
        String host = "localhost";
        int port = 8123;
        String user = "default";
        String password = "";
        String database = "default";
        String logComment = null;
        String sendLogsLevel = null;
        String maxInsertThreads = null;
        String query = null;
        boolean secure = false;
        boolean multiquery = false;
        Map<String, String> extraServerSettings = new LinkedHashMap<>();
        Path logPath = resolveLogPath();
        Options options = createBaseOptions();
        Set<String> knownLongOptions = collectLongOptionNames(options);

        appendLog(logPath, "=== clickhouse-client invocation ===");
        appendLog(logPath, "timestamp=" + new Date());
        appendLog(logPath, "argv=" + String.join(" ", args));

        registerDynamicLongOptions(options, knownLongOptions, args);

        CommandLineParser parser = DefaultParser.builder()
                .setAllowPartialMatching(false)
                .build();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            printUsage(options);
            System.exit(1);
            return;
        }

        if (cmd.hasOption("help")) {
            printUsage(options);
            System.exit(0);
        }

        host = cmd.getOptionValue("host", host);
        String portValue = cmd.getOptionValue("port");
        if (portValue != null) {
            port = Integer.parseInt(portValue);
        }
        user = cmd.getOptionValue("user", user);
        password = cmd.getOptionValue("password", password);
        database = cmd.getOptionValue("database", database);
        query = cmd.getOptionValue("query", query);
        logComment = firstNonNullOptionValue(cmd, "log_comment", "log-comment");
        sendLogsLevel = firstNonNullOptionValue(cmd, "send_logs_level", "send-logs-level");
        maxInsertThreads = firstNonNullOptionValue(cmd, "max_insert_threads", "max-insert-threads");
        secure = cmd.hasOption("secure");
        multiquery = cmd.hasOption("multiquery") || cmd.hasOption("multi-query");

        for (Option option : cmd.getOptions()) {
            String longOpt = option.getLongOpt();
            if (longOpt == null || knownLongOptions.contains(longOpt)) {
                continue;
            }
            String settingName = longOpt.replace('-', '_');
            String settingValue = option.hasArg() ? option.getValue() : null;
            if (settingValue == null) {
                settingValue = "1";
            }
            if (classifySetting(settingName) == SettingScope.SERVER) {
                extraServerSettings.put(settingName, settingValue);
            }
        }

        if (query == null) {
            query = readStdin();
        }

        if (query == null || query.isBlank()) {
            System.err.println("No query provided. Use --query or pipe SQL via stdin.");
            System.exit(1);
        }
        List<String> queries = multiquery ? splitQueries(query) : List.of(query);
        if (queries.isEmpty()) {
            System.err.println("No query provided. Use --query or pipe SQL via stdin.");
            System.exit(1);
        }

        String impl = System.getenv(IMPL_ENV);
        if (impl == null || impl.isBlank()) {
            impl = IMPL_CLIENT;
        }

        appendLog(logPath, "impl=" + impl);
        appendLog(logPath, "database=" + database + ", user=" + user + ", secure=" + secure + ", multiquery=" + multiquery);
        appendLog(logPath, "log_comment=" + safeForLog(logComment));
        appendLog(logPath, "send_logs_level=" + safeForLog(sendLogsLevel));
        appendLog(logPath, "max_insert_threads=" + safeForLog(maxInsertThreads));
        appendLog(logPath, "server_settings=" + extraServerSettings);
        appendLog(logPath, "queries_count=" + queries.size());
        for (int qi = 0; qi < queries.size(); qi++) {
            appendLog(logPath, "query[" + qi + "]=" + queries.get(qi));
        }

        try {
            switch (impl) {
                case IMPL_CLIENT:
                    executeWithClient(host, port, user, password, database, secure,
                            logComment, sendLogsLevel, maxInsertThreads,
                            extraServerSettings, queries, logPath);
                    break;
                case IMPL_JDBC:
                    executeWithJdbc(host, port, user, password, database, secure,
                            logComment, sendLogsLevel, maxInsertThreads,
                            extraServerSettings, queries, logPath);
                    break;
                default:
                    System.err.println("Unknown " + IMPL_ENV + " value: " + impl
                            + ". Supported: " + IMPL_CLIENT + ", " + IMPL_JDBC);
                    System.exit(1);
            }
        } catch (Exception e) {
            appendLog(logPath, "error=" + e.getMessage());
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void executeWithClient(String host, int port, String user, String password,
                                            String database, boolean secure,
                                            String logComment, String sendLogsLevel, String maxInsertThreads,
                                            Map<String, String> extraServerSettings,
                                            List<String> queries, Path logPath) throws Exception {
        String endpoint = (secure ? "https://" : "http://") + host + ":" + port;
        appendLog(logPath, "endpoint=" + endpoint);

        try (Client client = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .setDefaultDatabase(database)
                .build()) {

            QuerySettings settings = new QuerySettings()
                    .setFormat(ClickHouseFormat.TabSeparated);
            if (logComment != null && !logComment.isBlank()) {
                settings.logComment(logComment);
            }
            if (sendLogsLevel != null && !sendLogsLevel.isBlank()) {
                settings.serverSetting("send_logs_level", sendLogsLevel);
            }
            if (maxInsertThreads != null && !maxInsertThreads.isBlank()) {
                settings.serverSetting("max_insert_threads", maxInsertThreads);
            }
            for (Map.Entry<String, String> entry : extraServerSettings.entrySet()) {
                if (entry.getValue() != null && !entry.getValue().isBlank()) {
                    settings.serverSetting(entry.getKey(), entry.getValue());
                }
            }

            for (String q : queries) {
                appendLog(logPath, "executing_query=" + q);
                try (QueryResponse response = client.query(q, settings)
                        .get(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    try (InputStream is = response.getInputStream()) {
                        byte[] buf = new byte[8192];
                        int n;
                        while ((n = is.read(buf)) != -1) {
                            System.out.write(buf, 0, n);
                        }
                        System.out.flush();
                    }
                }
            }
        }
    }

    private static void executeWithJdbc(String host, int port, String user, String password,
                                         String database, boolean secure,
                                         String logComment, String sendLogsLevel, String maxInsertThreads,
                                         Map<String, String> extraServerSettings,
                                         List<String> queries, Path logPath) throws Exception {
        String protocol = secure ? "https" : "http";
        String jdbcUrl = "jdbc:clickhouse:" + protocol + "://" + host + ":" + port + "/" + database;
        appendLog(logPath, "jdbc_url=" + jdbcUrl);

        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);

        addServerSetting(props, "log_comment", logComment);
        addServerSetting(props, "send_logs_level", sendLogsLevel);
        addServerSetting(props, "max_insert_threads", maxInsertThreads);
        for (Map.Entry<String, String> entry : extraServerSettings.entrySet()) {
            addServerSetting(props, entry.getKey(), entry.getValue());
        }

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
             Statement stmt = conn.createStatement()) {

            for (String q : queries) {
                appendLog(logPath, "executing_query=" + q);
                boolean hasResultSet = stmt.execute(q);
                if (hasResultSet) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        writeResultSetAsTsv(rs);
                    }
                }
            }
        }
    }

    private static void addServerSetting(Properties props, String name, String value) {
        if (value != null && !value.isBlank()) {
            props.setProperty("clickhouse_setting_" + name, value);
        }
    }

    private static void writeResultSetAsTsv(ResultSet rs) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), CLICKHOUSE_TSV_FORMAT);
        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                printer.print(rs.getString(i));
            }
            printer.println();
        }
        printer.flush();
    }

    private static Options createBaseOptions() {
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("host").hasArg().argName("HOST")
                .desc("Server host (default: localhost)").build());
        options.addOption(Option.builder().longOpt("port").hasArg().argName("PORT")
                .desc("HTTP port (default: 8123)").build());
        options.addOption(Option.builder("u").longOpt("user").hasArg().argName("USER")
                .desc("Username (default: default)").build());
        options.addOption(Option.builder().longOpt("password").hasArg().argName("PASSWORD")
                .desc("Password (default: empty)").build());
        options.addOption(Option.builder("d").longOpt("database").hasArg().argName("DB")
                .desc("Database (default: default)").build());
        options.addOption(Option.builder("q").longOpt("query").hasArg().argName("SQL")
                .desc("SQL query to execute").build());
        options.addOption(Option.builder().longOpt("log_comment").hasArg().argName("VALUE")
                .desc("Comment for query_log records").build());
        options.addOption(Option.builder().longOpt("log-comment").hasArg().argName("VALUE").build());
        options.addOption(Option.builder().longOpt("send_logs_level").hasArg().argName("VALUE")
                .desc("Server log level to send with result").build());
        options.addOption(Option.builder().longOpt("send-logs-level").hasArg().argName("VALUE").build());
        options.addOption(Option.builder().longOpt("max_insert_threads").hasArg().argName("VALUE")
                .desc("Max insert threads setting").build());
        options.addOption(Option.builder().longOpt("max-insert-threads").hasArg().argName("VALUE").build());
        options.addOption(Option.builder("s").longOpt("secure")
                .desc("Use HTTPS").build());
        options.addOption(Option.builder("n").longOpt("multiline")
                .desc("(ignored, accepted for compatibility)").build());
        options.addOption(Option.builder().longOpt("multiquery")
                .desc("Execute multiple ';'-separated queries").build());
        options.addOption(Option.builder().longOpt("multi-query").build());
        options.addOption(Option.builder().longOpt("help")
                .desc("Print this help").build());
        return options;
    }

    private static Set<String> collectLongOptionNames(Options options) {
        Set<String> names = new HashSet<>();
        for (Option option : options.getOptions()) {
            if (option.getLongOpt() != null) {
                names.add(option.getLongOpt());
            }
        }
        return names;
    }

    private static void registerDynamicLongOptions(Options options, Set<String> knownLongOptions, String[] args) {
        Set<String> added = new LinkedHashSet<>();
        for (String arg : args) {
            if (arg == null || !arg.startsWith("--") || "--".equals(arg)) {
                continue;
            }
            int eq = arg.indexOf('=');
            String longOpt = eq >= 0 ? arg.substring(2, eq) : arg.substring(2);
            if (longOpt.isBlank() || knownLongOptions.contains(longOpt) || added.contains(longOpt)) {
                continue;
            }
            options.addOption(Option.builder()
                    .longOpt(longOpt)
                    .hasArg()
                    .optionalArg(true)
                    .argName("VALUE")
                    .build());
            added.add(longOpt);
        }
    }

    private static String firstNonNullOptionValue(CommandLine cmd, String... optionNames) {
        for (String optionName : optionNames) {
            String value = cmd.getOptionValue(optionName);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static Path resolveLogPath() {
        String fromEnv = System.getenv(LOG_PATH_ENV);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return Paths.get(fromEnv);
        }
        return DEFAULT_LOG_PATH;
    }

    private static void appendLog(Path path, String line) {
        String payload = line + System.lineSeparator();
        if (appendLogInternal(path, payload)) {
            return;
        }
        if (!FALLBACK_LOG_PATH.equals(path)) {
            appendLogInternal(FALLBACK_LOG_PATH, payload);
        }
    }

    private static boolean appendLogInternal(Path path, String payload) {
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
            try (FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND)) {
                channel.write(ByteBuffer.wrap(bytes));
                channel.force(true);
            }
            return true;
        } catch (Exception ignored) {
            // Logging must never break CLI behavior in tests.
            return false;
        }
    }

    private static String safeForLog(String value) {
        return value == null ? "<null>" : value;
    }

    private static String readStdin() {
        try {
            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (sb.length() > 0) {
                        sb.append('\n');
                    }
                    sb.append(line);
                }
            }
            return sb.isEmpty() ? null : sb.toString();
        } catch (IOException e) {
            return null;
        }
    }

    private static List<String> splitQueries(String sql) {
        List<String> queries = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inBacktick = false;
        boolean escaping = false;

        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);

            if (escaping) {
                current.append(ch);
                escaping = false;
                continue;
            }

            if ((inSingleQuote || inDoubleQuote) && ch == '\\') {
                current.append(ch);
                escaping = true;
                continue;
            }

            if (!inDoubleQuote && !inBacktick && ch == '\'') {
                inSingleQuote = !inSingleQuote;
                current.append(ch);
                continue;
            }
            if (!inSingleQuote && !inBacktick && ch == '"') {
                inDoubleQuote = !inDoubleQuote;
                current.append(ch);
                continue;
            }
            if (!inSingleQuote && !inDoubleQuote && ch == '`') {
                inBacktick = !inBacktick;
                current.append(ch);
                continue;
            }

            if (!inSingleQuote && !inDoubleQuote && !inBacktick && ch == ';') {
                String statement = current.toString().trim();
                if (!statement.isEmpty()) {
                    queries.add(statement);
                }
                current.setLength(0);
                continue;
            }

            current.append(ch);
        }

        String trailing = current.toString().trim();
        if (!trailing.isEmpty()) {
            queries.add(trailing);
        }

        return queries;
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setOptionComparator(null);
        formatter.printHelp("clickhouse-client [options]", USAGE_HEADER, options, "", true);
    }

    private static SettingScope classifySetting(String settingName) {
        if (SERVER_SETTINGS.contains(settingName)) {
            return SettingScope.SERVER;
        }
        if (CLIENT_ONLY_SETTINGS.contains(settingName)) {
            return SettingScope.CLIENT_ONLY;
        }
        return SettingScope.UNKNOWN;
    }

    private enum SettingScope {
        SERVER,
        CLIENT_ONLY,
        UNKNOWN
    }

    private static Set<String> createServerSettings() {
        Set<String> settings = new HashSet<>();
        Collections.addAll(settings,
                "max_insert_threads",
                "send_logs_level");
        return Collections.unmodifiableSet(settings);
    }

    private static Set<String> createClientOnlySettings() {
        Set<String> settings = new HashSet<>();
        Collections.addAll(settings,
                "group_by_two_level_threshold",
                "group_by_two_level_threshold_bytes",
                "distributed_aggregation_memory_efficient",
                "fsync_metadata",
                "output_format_parallel_formatting",
                "input_format_parallel_parsing",
                "min_chunk_bytes_for_parallel_parsing",
                "max_read_buffer_size",
                "prefer_localhost_replica",
                "max_block_size",
                "max_joined_block_size_rows",
                "joined_block_split_single_row",
                "join_output_by_rowlist_perkey_rows_threshold",
                "max_threads",
                "optimize_append_index",
                "use_hedged_requests",
                "optimize_if_chain_to_multiif",
                "optimize_if_transform_strings_to_enum",
                "optimize_read_in_order",
                "optimize_or_like_chain",
                "optimize_substitute_columns",
                "enable_multiple_prewhere_read_steps",
                "read_in_order_two_level_merge_threshold",
                "optimize_aggregation_in_order",
                "aggregation_in_order_max_block_bytes",
                "use_uncompressed_cache",
                "min_bytes_to_use_direct_io",
                "min_bytes_to_use_mmap_io",
                "local_filesystem_read_method",
                "remote_filesystem_read_method",
                "local_filesystem_read_prefetch",
                "filesystem_cache_segments_batch_size",
                "read_from_filesystem_cache_if_exists_otherwise_bypass_cache",
                "throw_on_error_from_cache_on_write_operations",
                "remote_filesystem_read_prefetch",
                "distributed_cache_discard_connection_if_unread_data",
                "distributed_cache_use_clients_cache_for_write",
                "distributed_cache_use_clients_cache_for_read",
                "allow_prefetched_read_pool_for_remote_filesystem",
                "filesystem_prefetch_max_memory_usage",
                "filesystem_prefetches_limit",
                "filesystem_prefetch_min_bytes_for_single_read_task",
                "filesystem_prefetch_step_marks",
                "filesystem_prefetch_step_bytes",
                "enable_filesystem_cache",
                "enable_filesystem_cache_on_write_operations",
                "compile_expressions",
                "compile_aggregate_expressions",
                "compile_sort_description",
                "merge_tree_coarse_index_granularity",
                "optimize_distinct_in_order",
                "max_bytes_before_remerge_sort",
                "min_compress_block_size",
                "max_compress_block_size",
                "merge_tree_compact_parts_min_granules_to_multibuffer_read",
                "optimize_sorting_by_input_stream_properties",
                "http_response_buffer_size",
                "http_wait_end_of_query",
                "enable_memory_bound_merging_of_aggregation_results",
                "min_count_to_compile_expression",
                "min_count_to_compile_aggregate_expression",
                "min_count_to_compile_sort_description",
                "session_timezone",
                "use_page_cache_for_disks_without_file_cache",
                "use_page_cache_for_local_disks",
                "use_page_cache_for_object_storage",
                "page_cache_inject_eviction",
                "merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability",
                "prefer_external_sort_block_bytes",
                "cross_join_min_rows_to_compress",
                "cross_join_min_bytes_to_compress",
                "min_external_table_block_size_bytes",
                "max_parsing_threads",
                "optimize_functions_to_subcolumns",
                "parallel_replicas_local_plan",
                "query_plan_join_swap_table",
                "enable_vertical_final",
                "optimize_extract_common_expressions",
                "optimize_syntax_fuse_functions",
                "use_async_executor_for_materialized_views",
                "use_query_condition_cache",
                "secondary_indices_enable_bulk_filtering",
                "use_skip_indexes_if_final",
                "use_skip_indexes_on_data_read",
                "optimize_rewrite_like_perfect_affix",
                "input_format_parquet_use_native_reader_v3",
                "enable_lazy_columns_replication",
                "allow_special_serialization_kinds_in_output_formats",
                "short_circuit_function_evaluation_for_nulls_threshold",
                "automatic_parallel_replicas_mode",
                "temporary_files_buffer_size",
                "query_plan_optimize_join_order_algorithm",
                "max_bytes_before_external_sort",
                "max_bytes_before_external_group_by",
                "max_bytes_ratio_before_external_sort",
                "max_bytes_ratio_before_external_group_by",
                "allow_repeated_settings",
                "use_skip_indexes_if_final_exact_mode",
                "ratio_of_defaults_for_sparse_serialization",
                "prefer_fetch_merged_part_size_threshold",
                "vertical_merge_algorithm_min_rows_to_activate",
                "vertical_merge_algorithm_min_columns_to_activate",
                "allow_vertical_merges_from_compact_to_wide_parts",
                "min_merge_bytes_to_use_direct_io",
                "index_granularity_bytes",
                "merge_max_block_size",
                "index_granularity",
                "min_bytes_for_wide_part",
                "compress_marks",
                "compress_primary_key",
                "marks_compress_block_size",
                "primary_key_compress_block_size",
                "replace_long_file_name_to_hash",
                "max_file_name_length",
                "min_bytes_for_full_part_storage",
                "compact_parts_max_bytes_to_buffer",
                "compact_parts_max_granules_to_buffer",
                "compact_parts_merge_max_bytes_to_prefetch_part",
                "cache_populated_by_fetch",
                "concurrent_part_removal_threshold",
                "old_parts_lifetime",
                "prewarm_mark_cache",
                "use_const_adaptive_granularity",
                "enable_index_granularity_compression",
                "enable_block_number_column",
                "enable_block_offset_column",
                "use_primary_key_cache",
                "prewarm_primary_key_cache",
                "object_serialization_version",
                "object_shared_data_serialization_version",
                "object_shared_data_serialization_version_for_zero_level_parts",
                "object_shared_data_buckets_for_compact_part",
                "object_shared_data_buckets_for_wide_part",
                "dynamic_serialization_version",
                "auto_statistics_types",
                "serialization_info_version",
                "string_serialization_version",
                "nullable_serialization_version",
                "enable_shared_storage_snapshot_in_query",
                "min_columns_to_activate_adaptive_write_buffer",
                "reduce_blocking_parts_sleep_ms",
                "shared_merge_tree_outdated_parts_group_size",
                "shared_merge_tree_max_outdated_parts_to_process_at_once");
        return Collections.unmodifiableSet(settings);
    }
}
