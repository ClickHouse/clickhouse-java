package com.clickhouse.client.cli;

import org.apache.commons.cli.ParseException;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A CLI that imitates {@code clickhouse-client} well enough for the ClickHouse core test runner
 * ({@code tests/clickhouse-test}) to drive it, and runs every query through clickhouse-java. It reads the result
 * values that the client decoded and writes them as {@code TabSeparated} text, so the upstream {@code .reference}
 * files check the decoding of the client.
 *
 * <p>The backend is selected by the {@code CLICKHOUSE_CLIENT_CLI_IMPL} environment variable:
 * <ul>
 *   <li>{@code client} (default): client-v2, values from {@code ClickHouseBinaryFormatReader.readValue}</li>
 *   <li>{@code jdbc}: jdbc-v2, values from {@code ResultSet.getObject}</li>
 *   <li>{@code text}: client-v2, the server's own text output copied as it is. Transport only: this backend does
 *       not check the decoding of the client.</li>
 * </ul>
 *
 * <p>See {@code README.md} for how to run the upstream SQL tests with it.
 */
public final class Main {

    static final String IMPL_ENV = "CLICKHOUSE_CLIENT_CLI_IMPL";

    private Main() {
    }

    public static void main(String[] args) {
        int code = run(args, System.in, System.out, System.err, System::getenv);
        System.out.flush();
        System.exit(code);
    }

    /** Runs one invocation. env gives the environment variables (the live tests give their own). */
    static int run(String[] argv, InputStream stdin, OutputStream stdout, PrintStream stderr,
                   Function<String, String> env) {
        InvocationLog log = new InvocationLog(env.apply(InvocationLog.LOG_PATH_ENV));
        try {
            return runLogged(argv, stdin, stdout, stderr, env, log);
        } finally {
            log.flush();
        }
    }

    private static int runLogged(String[] argv, InputStream stdin, OutputStream stdout, PrintStream stderr,
                                 Function<String, String> env, InvocationLog log) {
        log.add("argv", String.join(" ", argv));
        if (argv.length > 0 && argv[0].equals("extract-from-config")) {
            return extractFromConfig(argv, stdout);
        }
        CliOptions opts;
        try {
            opts = CliOptions.parse(argv);
        } catch (ParseException e) {
            stderr.print("Error: " + e.getMessage() + "\n\n" + CliOptions.usage());
            return 1;
        }
        if (opts.help) {
            write(stdout, CliOptions.usage());
            return 0;
        }
        String impl = env.apply(IMPL_ENV);
        if (impl == null || impl.isEmpty()) {
            impl = "client";
        }
        if (!impl.equals("client") && !impl.equals("jdbc") && !impl.equals("text")) {
            stderr.println("Error: unknown " + IMPL_ENV + " \"" + impl + "\": use client, jdbc or text");
            return 1;
        }
        log.add("impl", impl);

        String text = opts.query;
        if (text == null) {
            try {
                text = new String(readAll(stdin), StandardCharsets.UTF_8);
            } catch (IOException e) {
                stderr.println("Error: read stdin: " + e.getMessage());
                return 1;
            }
        }
        if (text.trim().isEmpty()) {
            stderr.println("Error: no query. Use --query or write the queries to stdin.");
            return 1;
        }
        List<Statement> statements = StatementSplitter.split(text);
        log.add("statements", String.valueOf(statements.size()));
        if (statements.isEmpty()) {
            return 0;
        }

        String sessionId = newSessionId();
        log.add("session_id", sessionId);
        int port = httpPort(opts.port, opts.secure, env);
        Map<String, String> settings = new LinkedHashMap<>(opts.settings);
        OutputStream out = new BufferedOutputStream(stdout, 1 << 16);
        Exception error = null;
        Backend backend = null;
        try {
            backend = impl.equals("jdbc")
                    ? new JdbcBackend(opts, port, sessionId, settings, log)
                    : new ClientBackend(opts, port, sessionId, impl.equals("text"), log);
            new Runner(backend, opts, settings, out, log).run(statements);
        } catch (Exception e) {
            error = e;
        }
        try {
            out.flush();
        } catch (IOException e) {
            error = error == null ? e : error;
        }
        if (backend != null) {
            try {
                backend.close();
            } catch (Exception e) {
                error = error == null ? e : error;
            }
        }
        if (error != null) {
            log.add("error", Errors.describe(error));
            return reportError(stderr, error);
        }
        return 0;
    }

    /**
     * Writes the error to stderr and returns the exit code. As in clickhouse-client, the exit code is the error
     * code modulo 256 when it is a server error.
     */
    static int reportError(PrintStream stderr, Throwable e) {
        int code = 0;
        if (e instanceof Errors.HintException) {
            stderr.println("Error: " + e.getMessage());
            code = ((Errors.HintException) e).code;
        } else {
            Integer serverCode = Errors.serverCode(e);
            if (serverCode != null) {
                stderr.println("Received exception from server:\n" + Errors.formatServerError(e, serverCode));
                code = serverCode;
            } else {
                stderr.println("Error: " + Errors.describe(e));
            }
        }
        stderr.flush();
        return code % 256 != 0 ? code % 256 : 1;
    }

    /**
     * Maps the port that {@code tests/clickhouse-test} gives (the native TCP port, from CLICKHOUSE_PORT_TCP) to the
     * HTTP port that the runner exports. Any other port is used as given.
     */
    static int httpPort(Integer port, boolean secure, Function<String, String> env) {
        String nativeEnv = secure ? "CLICKHOUSE_PORT_TCP_SECURE" : "CLICKHOUSE_PORT_TCP";
        String httpEnv = secure ? "CLICKHOUSE_PORT_HTTPS" : "CLICKHOUSE_PORT_HTTP";
        int nativeDefault = secure ? 9440 : 9000;
        int httpDefault = secure ? 8443 : 8123;
        int nativePort = envInt(env.apply(nativeEnv), nativeDefault);
        if (port != null && port != 0 && port != nativePort) {
            return port;
        }
        return envInt(env.apply(httpEnv), httpDefault);
    }

    private static int envInt(String v, int def) {
        if (v == null) {
            return def;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static String newSessionId() {
        byte[] b = new byte[16];
        new SecureRandom().nextBytes(b);
        StringBuilder sb = new StringBuilder("clickhouse-java-cli-");
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }

    /**
     * Answers {@code clickhouse extract-from-config --key <key>}, which {@code tests/clickhouse-test} runs to read
     * the server config. Only {@code listen_host} has an answer; for other keys the output is empty, as for a key
     * that is not in the config. ({@code bin/clickhouse} answers it without a JVM; this is for direct use.)
     */
    static int extractFromConfig(String[] argv, OutputStream stdout) {
        String key = null;
        for (int i = 1; i < argv.length; i++) {
            if (argv[i].equals("--key") && i + 1 < argv.length) {
                key = argv[++i];
            } else if (argv[i].startsWith("--key=")) {
                key = argv[i].substring("--key=".length());
            }
        }
        if ("listen_host".equals(key)) {
            write(stdout, "127.0.0.1\n");
        }
        return 0;
    }

    private static void write(OutputStream out, String s) {
        try {
            out.write(s.getBytes(StandardCharsets.UTF_8));
            out.flush();
        } catch (IOException e) {
            // Nothing to do: stdout is gone.
        }
    }

    private static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        byte[] b = new byte[8192];
        int n;
        while ((n = in.read(b)) != -1) {
            buf.write(b, 0, n);
        }
        return buf.toByteArray();
    }
}
