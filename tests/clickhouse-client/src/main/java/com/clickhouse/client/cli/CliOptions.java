package com.clickhouse.client.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** The {@code clickhouse-client} command line, as far as {@code tests/clickhouse-test} uses it. */
final class CliOptions {

    /**
     * The command-line settings that the shim sends to the server. {@code tests/clickhouse-test} also adds many
     * randomized settings to the command line. The shim drops them: they only change performance-related paths in
     * the server, and some of them are client-only.
     *
     * <p>{@code async_insert} and {@code enable_parallel_replicas} are not randomized: {@code tests/clickhouse-test}
     * sets them to 0 for a test with the tag {@code no-async-insert} or {@code no-parallel-replicas}
     * ({@code FEATURE_TAG_OVERRIDES}), because the test does not work with the feature. The server default of
     * {@code async_insert} is 1, so the shim must send them.
     */
    static final Set<String> SERVER_SETTINGS = new HashSet<>(Arrays.asList(
            "log_comment", "max_insert_threads", "max_threads", "send_logs_level",
            "async_insert", "enable_parallel_replicas"));

    String host = "localhost";
    /** The port of the command line, or null when not given. */
    Integer port;
    String user = "default";
    String password = "";
    String database = "default";
    String query;
    boolean secure;
    boolean help;
    /** The command-line settings that go to the server, in command-line order. */
    final Map<String, String> settings = new LinkedHashMap<>();
    /** The query parameters from {@code --param_<name>=<value>}. */
    final Map<String, String> params = new LinkedHashMap<>();

    static Options baseOptions() {
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("host").hasArg().argName("HOST")
                .desc("Server host (default: localhost)").build());
        options.addOption(Option.builder().longOpt("port").hasArg().argName("PORT")
                .desc("Server port. The native port of the test runner (CLICKHOUSE_PORT_TCP) maps to"
                        + " CLICKHOUSE_PORT_HTTP (default: 8123)").build());
        options.addOption(Option.builder("u").longOpt("user").hasArg().argName("USER")
                .desc("User name (default: default)").build());
        options.addOption(Option.builder().longOpt("password").hasArg().argName("PASSWORD")
                .desc("Password (default: empty)").build());
        options.addOption(Option.builder("d").longOpt("database").hasArg().argName("DB")
                .desc("Database (default: default)").build());
        options.addOption(Option.builder("q").longOpt("query").hasArg().argName("SQL")
                .desc("Queries to run; without it, the queries are read from stdin").build());
        options.addOption(Option.builder("s").longOpt("secure").desc("Use HTTPS").build());
        options.addOption(Option.builder().longOpt("no-secure").desc("Use HTTP (default)").build());
        // As in clickhouse-client: `-n` is `--multiquery`, `-m` is `--multiline`. The shim always runs every
        // statement of the input.
        options.addOption(Option.builder("n").longOpt("multiquery").desc("Ignored").build());
        options.addOption(Option.builder().longOpt("multi-query").desc("Ignored").build());
        options.addOption(Option.builder("m").longOpt("multiline").desc("Ignored").build());
        options.addOption(Option.builder().longOpt("help").desc("Show this help").build());
        // Accepted and ignored.
        options.addOption(Option.builder("C").longOpt("config-file").hasArg().argName("FILE").desc("Ignored").build());
        options.addOption(Option.builder().longOpt("client_logs_file").hasArg().argName("FILE").desc("Ignored").build());
        options.addOption(Option.builder().longOpt("allow_repeated_settings").desc("Ignored").build());
        return options;
    }

    static CliOptions parse(String[] argv) throws ParseException {
        Options options = baseOptions();
        Set<String> known = new HashSet<>();
        for (Option o : options.getOptions()) {
            if (o.getLongOpt() != null) {
                known.add(o.getLongOpt());
            }
        }
        // Any other long option is a setting (`--max_threads=1`) or a query parameter (`--param_x=1`), with an
        // optional value. Unknown short options are ignored, as in the other language ports.
        List<String> args = new ArrayList<>();
        for (String arg : argv) {
            if (arg.startsWith("--") && arg.length() > 2) {
                int eq = arg.indexOf('=');
                String name = eq >= 0 ? arg.substring(2, eq) : arg.substring(2);
                if (!name.isEmpty() && !known.contains(name) && !options.hasLongOption(name)) {
                    options.addOption(Option.builder().longOpt(name).hasArg().optionalArg(true).build());
                }
            } else if (arg.length() > 1 && arg.charAt(0) == '-' && Character.isLetter(arg.charAt(1))
                    && !options.hasShortOption(arg.substring(1, 2))) {
                continue;
            }
            args.add(arg);
        }

        CommandLine cmd = DefaultParser.builder().setAllowPartialMatching(false).build()
                .parse(options, args.toArray(new String[0]));
        CliOptions o = new CliOptions();
        o.help = cmd.hasOption("help");
        o.host = cmd.getOptionValue("host", o.host);
        String port = cmd.getOptionValue("port");
        if (port != null) {
            try {
                o.port = Integer.valueOf(port);
            } catch (NumberFormatException e) {
                throw new ParseException("bad --port value \"" + port + "\"");
            }
        }
        o.user = cmd.getOptionValue("user", o.user);
        o.password = cmd.getOptionValue("password", o.password);
        o.database = cmd.getOptionValue("database", o.database);
        o.query = cmd.getOptionValue("query");
        for (Option opt : cmd.getOptions()) {
            String name = opt.getLongOpt();
            if ("secure".equals(name)) {
                o.secure = true;
            } else if ("no-secure".equals(name)) {
                o.secure = false;
            }
            if (name == null || known.contains(name)) {
                continue;
            }
            String value = opt.getValue() == null ? "1" : opt.getValue();
            if (name.startsWith("param_")) {
                o.params.put(name.substring("param_".length()), value);
                continue;
            }
            String setting = name.replace('-', '_');
            if (SERVER_SETTINGS.contains(setting)) {
                o.settings.put(setting, value);
            }
        }
        return o;
    }

    static String usage() {
        StringWriter sw = new StringWriter();
        HelpFormatter formatter = new HelpFormatter();
        formatter.setOptionComparator(null);
        String header = "\nA clickhouse-client imitation that runs queries through clickhouse-java, so that the"
                + " ClickHouse tests/clickhouse-test runner can drive it. Every \";\"-separated statement runs."
                + " --SETTING=VALUE: log_comment, max_insert_threads, max_threads, send_logs_level, async_insert"
                + " and enable_parallel_replicas go to the server, other settings are ignored."
                + " --param_NAME=VALUE: a query parameter.\n\n";
        String footer = "\nEnvironment variables:\n"
                + "  CLICKHOUSE_CLIENT_CLI_IMPL  client (default, client-v2), jdbc (jdbc-v2) or text (transport only)\n"
                + "  CLICKHOUSE_CLIENT_CLI_LOG   Log file for troubleshooting\n";
        formatter.printHelp(new PrintWriter(sw), 100, "clickhouse-client [options]", header, baseOptions(), 2, 2,
                footer, true);
        return sw.toString();
    }
}
