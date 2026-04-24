package com.clickhouse.examples.jdbc_v2.json_processors;

import com.clickhouse.client.api.ClientConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

public class JdbcV2JsonProcessorsExample {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcV2JsonProcessorsExample.class);
    private static final List<String> SUPPORTED_PROCESSORS = Arrays.asList("JACKSON", "GSON");

    public static void main(String[] args) throws Exception {
        String url = System.getProperty("chUrl", "jdbc:clickhouse://localhost:8123/default");
        String user = System.getProperty("chUser", "default");
        String password = System.getProperty("chPassword", "");

        for (String processor : requestedProcessors()) {
            runExample(url, user, password, processor);
        }
    }

    private static void runExample(String url, String user, String password, String processor) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty(ClientConfigProperties.JSON_PROCESSOR.getKey(), processor);

        LOG.info("Running jdbc-v2 JSONEachRow example with processor {}", processor);

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT number + 1 AS id, concat('processor-', toString(number + 1)) AS label "
                             + "FROM numbers(3) FORMAT JSONEachRow")) {
            while (rs.next()) {
                LOG.info("[{}] id={}, label={}", processor, rs.getInt("id"), rs.getString("label"));
            }
        }
    }

    private static List<String> requestedProcessors() {
        String requested = System.getProperty("jsonProcessor", "").trim();
        if (requested.isEmpty()) {
            return SUPPORTED_PROCESSORS;
        }

        String normalized = requested.toUpperCase(Locale.ROOT);
        if (!SUPPORTED_PROCESSORS.contains(normalized)) {
            throw new IllegalArgumentException("Unsupported jsonProcessor '" + requested
                    + "'. Expected one of: " + SUPPORTED_PROCESSORS);
        }

        return Collections.singletonList(normalized);
    }
}
