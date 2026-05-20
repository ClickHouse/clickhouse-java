package com.clickhouse.examples.jdbc;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reproducer for issue #2361 — ConnectionClosedException: "Premature end of
 * chunk coded message body: closing chunk expected" during ResultSet.close().
 *
 * <p>Recipe:
 * <ol>
 *   <li>Configure the JDBC connection with LZ4 HTTP compression on
 *       ({@code compress=true&client.use_http_compression=true}) and a
 *       short server-side {@code send_timeout=1} so the server abandons
 *       writes after one second of back-pressure.</li>
 *   <li>Force the server to commit to chunked HTTP encoding by buffering
 *       the full response: {@code http_response_buffer_size=104857600}
 *       and {@code wait_end_of_query=1}.</li>
 *   <li>Issue a query whose result is large enough to overflow the buffer
 *       and that a slow client cannot drain in time.</li>
 *   <li>Read rows slowly (a few {@code Thread.sleep}s per thousand rows).</li>
 * </ol>
 *
 * <p>The server hits {@code SOCKET_TIMEOUT} writing the chunked body, closes
 * the connection without emitting the terminating zero-length chunk, and
 * the driver's stream-drain on close trips the exception.
 *
 * <p>Run with: {@code java -DchUrl=jdbc:ch://localhost:8123 com.clickhouse.examples.jdbc.Issue2361Repro [iters]}
 *
 * <p>The fix in {@code ResultSetImpl.close()} downgrades this drain-time
 * failure to a debug log instead of a thrown SQLException — close() should
 * never punish callers for a server-side teardown race after iteration is done.
 */
public class Issue2361Repro {

    // 5M rows × ~4.5KB pad ≈ 22 GB uncompressed (~700 MB lz4-compressed).
    // Large enough that send_timeout=1 + slow client reliably triggers the bug
    // in a freshly-started server (before its memory tracker fills with prior
    // query state).
    static final String QUERY =
            "SELECT number, repeat('xyz', 1500) AS pad FROM numbers(5000000)";

    public static void main(String[] args) throws Exception {
        Class.forName("com.clickhouse.jdbc.Driver");

        String baseUrl = System.getProperty("chUrl", "jdbc:ch://localhost:8123") + "/default";
        int iters = args.length > 0 ? Integer.parseInt(args[0]) : 3;

        Properties props = new Properties();
        props.setProperty("user", System.getProperty("chUser", "default"));
        props.setProperty("password", System.getProperty("chPassword", ""));
        // Compression on — same lz4-framed-over-HTTP path observed in the bug.
        props.setProperty("compress", "true");
        props.setProperty("client.use_http_compression", "true");
        // Long socket timeout so we don't bail out before the bug surfaces.
        props.setProperty("socket_timeout", "60000");
        // Trigger conditions:
        props.setProperty("clickhouse_setting_send_timeout", "1");
        props.setProperty("clickhouse_setting_http_response_buffer_size", "104857600");
        props.setProperty("clickhouse_setting_wait_end_of_query", "1");
        props.setProperty("clickhouse_setting_max_execution_time", "120");

        AtomicInteger trips = new AtomicInteger();
        String firstStack = null;

        for (int i = 0; i < iters; i++) {
            long t0 = System.currentTimeMillis();
            try (Connection c = DriverManager.getConnection(baseUrl, props);
                 Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery(QUERY)) {

                int rows = 0;
                try {
                    while (rs.next()) {
                        rs.getString(2);
                        rows++;
                        // Slow client: ~1 s per ~1000 rows → 5,000 s to fully
                        // drain. Server send_timeout=1 will fire well before then.
                        if (rows % 100 == 0) Thread.sleep(50);
                    }
                } catch (SQLException re) {
                    // Mid-iteration errors land here; the bug we want is the
                    // try-with-resources close that fires after the catch.
                }
            } catch (Throwable t) {
                if (hasInMsg(t, "Premature end of chunk")) {
                    trips.incrementAndGet();
                    if (firstStack == null) firstStack = stack(t);
                }
            }
            System.out.printf("iter %d: trips=%d elapsed=%dms%n",
                    i, trips.get(), System.currentTimeMillis() - t0);
            System.out.flush();
        }

        System.out.printf("%nFINAL: trips=%d / %d iterations (%.0f%%)%n",
                trips.get(), iters, 100.0 * trips.get() / iters);
        if (firstStack != null) {
            System.out.println("\n-- first failure stack (top frames) --");
            String[] lines = firstStack.split("\n");
            for (int li = 0; li < Math.min(24, lines.length); li++) {
                System.out.println(lines[li]);
            }
        }
    }

    private static boolean hasInMsg(Throwable t, String needle) {
        while (t != null) {
            if (t.getMessage() != null && t.getMessage().contains(needle)) return true;
            t = t.getCause();
        }
        return false;
    }

    private static String stack(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
