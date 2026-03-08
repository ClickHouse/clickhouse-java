package com.clickhouse.client.cli;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

        appendLog(logPath, "=== clickhouse-client invocation ===");
        appendLog(logPath, "timestamp=" + new Date());
        appendLog(logPath, "argv=" + String.join(" ", args));

        for (int i = 0; i < args.length; i++) {
            ParsedOption option = parseOption(args[i]);
            String argName = option.name;
            String inlineValue = option.inlineValue;
            switch (argName) {
                case "--host":
                case "-h":
                    host = valueOrNextArg(args, i, argName, inlineValue);
                    if (inlineValue == null) {
                        i++;
                    }
                    break;
                case "--port":
                    port = Integer.parseInt(valueOrNextArg(args, i, "--port", inlineValue));
                    if (inlineValue == null) {
                        i++;
                    }
                    break;
                case "--user":
                case "-u":
                    user = valueOrNextArg(args, i, argName, inlineValue);
                    if (inlineValue == null) {
                        i++;
                    }
                    break;
                case "--password":
                    password = valueOrNextArg(args, i, "--password", inlineValue);
                    if (inlineValue == null) {
                        i++;
                    }
                    break;
                case "--database":
                case "-d":
                    database = valueOrNextArg(args, i, argName, inlineValue);
                    if (inlineValue == null) {
                        i++;
                    }
                    break;
                case "--query":
                case "-q":
                    query = valueOrNextArg(args, i, argName, inlineValue);
                    if (inlineValue == null) {
                        i++;
                    }
                    break;
                case "--log_comment":
                case "--log-comment":
                    logComment = valueOrNextArg(args, i, argName, inlineValue);
                    if (inlineValue == null) {
                        i++;
                    }
                    break;
                case "--send_logs_level":
                case "--send-logs-level":
                    sendLogsLevel = valueOrNextArg(args, i, argName, inlineValue);
                    if (inlineValue == null) {
                        i++;
                    }
                    break;
                case "--max_insert_threads":
                case "--max-insert-threads":
                    maxInsertThreads = valueOrNextArg(args, i, argName, inlineValue);
                    if (inlineValue == null) {
                        i++;
                    }
                    break;
                case "--secure":
                case "-s":
                    secure = true;
                    break;
                case "--multiline":
                case "-n":
                    break;
                case "--multiquery":
                case "--multi-query":
                    multiquery = true;
                    break;
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
                default:
                    if (argName.startsWith("--")) {
                        String settingName = argName.substring(2).replace('-', '_');
                        String settingValue = inlineValue;
                        if (settingValue == null && hasNextValueToken(args, i)) {
                            settingValue = args[++i];
                        }
                        if (settingValue == null) {
                            // Keep compatibility with flag-style options that have no explicit value.
                            settingValue = "1";
                        }
                        SettingScope scope = classifySetting(settingName);
                        if (scope == SettingScope.SERVER) {
                            extraServerSettings.put(settingName, settingValue);
                        }
                    } else if (isDetachedValueToken(argName)) {
                        // Some test runners may accidentally shift argv tokenization.
                        // For compatibility, ignore standalone value tokens.
                    } else {
                        System.err.println("Unknown option: " + args[i]);
                        printArgContext(args, i);
                        printUsage();
                        System.exit(1);
                    }
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
        StringBuilder line = new StringBuilder();

        while (rs.next()) {
            line.setLength(0);
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    line.append('\t');
                }
                String val = rs.getString(i);
                if (val == null) {
                    line.append("\\N");
                } else {
                    escapeTsv(val, line);
                }
            }
            line.append('\n');
            System.out.print(line);
        }
        System.out.flush();
    }

    private static void escapeTsv(String value, StringBuilder out) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\':
                    out.append('\\').append('\\');
                    break;
                case '\t':
                    out.append('\\').append('t');
                    break;
                case '\n':
                    out.append('\\').append('n');
                    break;
                default:
                    out.append(ch);
            }
        }
    }

    private static String nextArg(String[] args, int currentIndex, String flag) {
        int nextIndex = currentIndex + 1;
        if (nextIndex >= args.length) {
            System.err.println("Missing value for " + flag);
            printArgContext(args, currentIndex);
            System.exit(1);
        }
        String nextToken = args[nextIndex];
        if (nextToken.startsWith("--")) {
            System.err.println("Missing value for " + flag);
            printArgContext(args, currentIndex);
            System.exit(1);
        }
        return nextToken;
    }

    private static String valueOrNextArg(String[] args, int currentIndex, String flag, String inlineValue) {
        if (inlineValue != null) {
            return inlineValue;
        }
        return nextArg(args, currentIndex, flag);
    }

    private static ParsedOption parseOption(String arg) {
        if (arg.startsWith("-")) {
            int eq = arg.indexOf('=');
            if (eq > 0) {
                return new ParsedOption(arg.substring(0, eq), arg.substring(eq + 1));
            }
        }
        return new ParsedOption(arg, null);
    }

    private static boolean hasNextValueToken(String[] args, int currentIndex) {
        int nextIndex = currentIndex + 1;
        if (nextIndex >= args.length) {
            return false;
        }
        String nextToken = args[nextIndex];
        return !nextToken.startsWith("--");
    }

    private static boolean isDetachedValueToken(String token) {
        return token != null && !token.isEmpty() && !token.startsWith("-");
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

    private static void printArgContext(String[] args, int currentIndex) {
        int contextSize = 6;
        int start = Math.max(0, currentIndex - contextSize);
        int end = Math.min(args.length - 1, currentIndex + 1);
        StringBuilder sb = new StringBuilder();
        sb.append("Argument context: ");
        for (int j = start; j <= end; j++) {
            if (j > start) {
                sb.append(' ');
            }
            if (j == currentIndex) {
                sb.append(">>").append(args[j]).append("<<");
            } else {
                sb.append(args[j]);
            }
        }
        System.err.println(sb.toString());
    }

    private static final class ParsedOption {
        private final String name;
        private final String inlineValue;

        private ParsedOption(String name, String inlineValue) {
            this.name = name;
            this.inlineValue = inlineValue;
        }
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

    private static void printUsage() {
        System.err.println("Usage: clickhouse-client [options]");
        System.err.println();
        System.err.println("Options:");
        System.err.println("  --host, -h       Server host (default: localhost)");
        System.err.println("  --port           HTTP port (default: 8123)");
        System.err.println("  --user, -u       Username (default: default)");
        System.err.println("  --password       Password (default: empty)");
        System.err.println("  --database, -d   Database (default: default)");
        System.err.println("  --query, -q      SQL query to execute");
        System.err.println("  --log_comment    Comment for query_log records");
        System.err.println("  --send_logs_level Server log level to send with result");
        System.err.println("  --max_insert_threads Max insert threads setting");
        System.err.println("  --multiquery     Execute multiple ';'-separated queries");
        System.err.println("  --secure, -s     Use HTTPS");
        System.err.println("  --help           Print this help");
        System.err.println();
        System.err.println("Both '--option value' and '--option=value' formats are supported.");
        System.err.println("Known server settings are forwarded to ClickHouse.");
        System.err.println("Client-only and unknown settings are accepted but not sent to server.");
        System.err.println("If --query is not specified, the query is read from stdin.");
        System.err.println();
        System.err.println("Environment variables:");
        System.err.println("  CLICKHOUSE_CLIENT_CLI_IMPL   Backend implementation: 'client' (default) or 'jdbc'");
        System.err.println("  CLICKHOUSE_CLIENT_CLI_LOG    Path to log file for troubleshooting");
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
