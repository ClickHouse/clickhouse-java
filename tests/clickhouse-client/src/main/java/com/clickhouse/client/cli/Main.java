package com.clickhouse.client.cli;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Simple CLI tool that mimics clickhouse-client.
 * Executes a SQL query against a ClickHouse server and prints results in TSV format.
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
                        String settingValue = valueOrNextArg(args, i, argName, inlineValue);
                        extraServerSettings.put(settingName, settingValue);
                        if (inlineValue == null) {
                            i++;
                        }
                    } else {
                        System.err.println("Unknown option: " + args[i]);
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

        String endpoint = (secure ? "https://" : "http://") + host + ":" + port;

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
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static String nextArg(String[] args, int currentIndex, String flag) {
        int nextIndex = currentIndex + 1;
        if (nextIndex >= args.length) {
            System.err.println("Missing value for " + flag);
            System.exit(1);
        }
        return args[nextIndex];
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
        System.err.println("Unknown '--name value' options are passed through as server settings.");
        System.err.println("If --query is not specified, the query is read from stdin.");
    }
}
