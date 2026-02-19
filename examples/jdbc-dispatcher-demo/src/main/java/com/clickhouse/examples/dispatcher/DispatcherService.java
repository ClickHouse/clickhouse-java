package com.clickhouse.examples.dispatcher;

import com.clickhouse.jdbc.dispatcher.DispatcherDriver;
import com.clickhouse.jdbc.dispatcher.DriverVersion;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * A simple HTTP backend service demonstrating jdbc-dispatcher usage.
 * Uses only JDK built-in HTTP server (com.sun.net.httpserver).
 * 
 * <p>Endpoints:
 * <ul>
 *   <li>GET /health - Health check</li>
 *   <li>GET /version - ClickHouse server version</li>
 *   <li>GET /drivers - List loaded driver versions</li>
 *   <li>GET /query?sql=SELECT... - Execute a query and return JSON results</li>
 * </ul>
 * 
 * <p>Start ClickHouse first:
 * <pre>docker run -d -p 8123:8123 --name clickhouse clickhouse/clickhouse-server</pre>
 */
public class DispatcherService {

    private static final Logger log = LoggerFactory.getLogger(DispatcherService.class);

    // Configuration
    private static final int HTTP_PORT = 8080;
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/default";
    private static final String DRIVERS_DIR = "drivers";
    private static final String DRIVER_CLASS_NAME = "com.clickhouse.jdbc.ClickHouseDriver";

    private final HttpServer server;
    private final DispatcherDriver dispatcher;
    private final Properties connectionProps;

    public static void main(String[] args) throws Exception {
        DispatcherService service = new DispatcherService();
        service.start();

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            service.stop();
        }));

        log.info("Service started on http://localhost:{}", HTTP_PORT);
        log.info("Endpoints:");
        log.info("  GET /health  - Health check");
        log.info("  GET /version - ClickHouse version");
        log.info("  GET /drivers - Loaded driver versions");
        log.info("  GET /query?sql=SELECT... - Execute query");
        log.info("");
        log.info("Press Ctrl+C to stop");

        // Keep the main thread alive
        Thread.currentThread().join();
    }

    public DispatcherService() throws IOException {
        // Initialize dispatcher with drivers
        this.dispatcher = initializeDispatcher();

        // Connection properties
        this.connectionProps = new Properties();
        connectionProps.setProperty("user", "default");
        connectionProps.setProperty("password", "");

        // Create HTTP server
        this.server = HttpServer.create(new InetSocketAddress(HTTP_PORT), 0);
        server.setExecutor(Executors.newFixedThreadPool(10));

        // Register endpoints
        server.createContext("/health", new HealthHandler());
        server.createContext("/version", new VersionHandler());
        server.createContext("/drivers", new DriversHandler());
        server.createContext("/query", new QueryHandler());
    }

    private DispatcherDriver initializeDispatcher() {
        File driversDir = new File(DRIVERS_DIR);
        if (!driversDir.exists() || !driversDir.isDirectory()) {
            throw new IllegalStateException("Drivers directory not found: " + driversDir.getAbsolutePath() +
                    ". Run './gradlew downloadDrivers' first.");
        }

        DispatcherDriver driver = new DispatcherDriver(DRIVER_CLASS_NAME);
        int loaded = driver.loadFromDirectory(driversDir);
        log.info("Loaded {} driver versions from {}", loaded, driversDir.getAbsolutePath());

        return driver;
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(1);
    }

    // ============ HTTP Handlers ============

    /**
     * GET /health - Simple health check
     */
    class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }

            String response = jsonObject(
                    "status", "ok",
                    "service", "jdbc-dispatcher-demo"
            );
            sendJson(exchange, 200, response);
        }
    }

    /**
     * GET /version - Returns ClickHouse server version
     */
    class VersionHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }

            try (Connection conn = dispatcher.connect(CLICKHOUSE_URL, connectionProps);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT version() AS version, now() AS server_time")) {

                if (rs.next()) {
                    String response = jsonObject(
                            "clickhouse_version", rs.getString("version"),
                            "server_time", rs.getString("server_time"),
                            "status", "connected"
                    );
                    sendJson(exchange, 200, response);
                } else {
                    sendError(exchange, 500, "No result from version query");
                }
            } catch (SQLException e) {
                log.error("Database error", e);
                sendError(exchange, 500, "Database error: " + e.getMessage());
            }
        }
    }

    /**
     * GET /drivers - Lists all loaded driver versions
     */
    class DriversHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }

            StringBuilder driversJson = new StringBuilder("[");
            boolean first = true;

            for (DriverVersion version : dispatcher.getVersionManager().getVersions()) {
                if (!first) driversJson.append(",");
                first = false;

                driversJson.append(jsonObject(
                        "version", version.getVersion(),
                        "healthy", String.valueOf(version.isHealthy()),
                        "major", String.valueOf(version.getMajorVersion()),
                        "minor", String.valueOf(version.getMinorVersion())
                ));
            }
            driversJson.append("]");

            DriverVersion newest = dispatcher.getVersionManager().getNewestVersion();
            String response = "{" +
                    "\"drivers\":" + driversJson + "," +
                    "\"newest\":\"" + (newest != null ? newest.getVersion() : "none") + "\"," +
                    "\"count\":" + dispatcher.getVersionManager().getVersions().size() +
                    "}";

            sendJson(exchange, 200, response);
        }
    }

    /**
     * GET /query?sql=SELECT... - Executes a query and returns JSON results
     */
    class QueryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }

            // Parse query parameters
            Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
            String sql = params.get("sql");

            if (sql == null || sql.isBlank()) {
                sendError(exchange, 400, "Missing 'sql' parameter. Usage: /query?sql=SELECT...");
                return;
            }

            // Security: only allow SELECT queries
            if (!sql.trim().toUpperCase().startsWith("SELECT")) {
                sendError(exchange, 400, "Only SELECT queries are allowed");
                return;
            }

            log.info("Executing query: {}", sql);

            try (Connection conn = dispatcher.connect(CLICKHOUSE_URL, connectionProps);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {

                String response = resultSetToJson(rs);
                sendJson(exchange, 200, response);

            } catch (SQLException e) {
                log.error("Query error", e);
                sendError(exchange, 500, "Query error: " + e.getMessage());
            }
        }
    }

    // ============ Helper Methods ============

    private Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null || query.isBlank()) {
            return params;
        }

        for (String param : query.split("&")) {
            String[] pair = param.split("=", 2);
            if (pair.length == 2) {
                String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
                String value = URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
                params.put(key, value);
            }
        }
        return params;
    }

    private String resultSetToJson(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        // Build column names array
        StringBuilder columns = new StringBuilder("[");
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) columns.append(",");
            columns.append("\"").append(escapeJson(meta.getColumnName(i))).append("\"");
        }
        columns.append("]");

        // Build rows array
        StringBuilder rows = new StringBuilder("[");
        boolean firstRow = true;
        int rowCount = 0;

        while (rs.next() && rowCount < 1000) { // Limit to 1000 rows
            if (!firstRow) rows.append(",");
            firstRow = false;
            rowCount++;

            rows.append("[");
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) rows.append(",");
                Object value = rs.getObject(i);
                if (value == null) {
                    rows.append("null");
                } else if (value instanceof Number) {
                    rows.append(value);
                } else if (value instanceof Boolean) {
                    rows.append(value);
                } else {
                    rows.append("\"").append(escapeJson(value.toString())).append("\"");
                }
            }
            rows.append("]");
        }
        rows.append("]");

        return "{" +
                "\"columns\":" + columns + "," +
                "\"rows\":" + rows + "," +
                "\"row_count\":" + rowCount +
                "}";
    }

    private String jsonObject(String... keyValues) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < keyValues.length; i += 2) {
            if (i > 0) sb.append(",");
            String key = keyValues[i];
            String value = keyValues[i + 1];

            sb.append("\"").append(key).append("\":");

            // Check if value looks like a boolean or number
            if ("true".equals(value) || "false".equals(value)) {
                sb.append(value);
            } else if (value.matches("-?\\d+(\\.\\d+)?")) {
                sb.append(value);
            } else {
                sb.append("\"").append(escapeJson(value)).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private void sendJson(HttpExchange exchange, int statusCode, String json) throws IOException {
        byte[] response = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }

    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        String json = jsonObject("error", message, "status", String.valueOf(statusCode));
        sendJson(exchange, statusCode, json);
    }
}
