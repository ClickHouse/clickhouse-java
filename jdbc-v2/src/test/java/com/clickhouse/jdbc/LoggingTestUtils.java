package com.clickhouse.jdbc;

import org.slf4j.LoggerFactory;
import org.slf4j.simple.SimpleLogger;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Captures the output of slf4j-simple, the test logging backend, with chosen loggers at a chosen level.
 */
public final class LoggingTestUtils {

    /** An action that may throw; its exception is rethrown after the output is captured. */
    public interface Action {
        void run() throws Exception;
    }

    private LoggingTestUtils() {
    }

    /**
     * Runs {@code action} with every logger in {@code loggers} at {@code level} and returns what was logged.
     * slf4j-simple caches loggers by name, so the level applies to a logger whether or not its class is loaded yet.
     * slf4j-simple writes to {@code System.err} and looks it up for every message, so it is swapped for the capture.
     */
    public static String captureLogs(List<String> loggers, int level, Action action) throws Exception {
        Field levelField = SimpleLogger.class.getDeclaredField("currentLogLevel");
        levelField.setAccessible(true);
        Map<Object, Integer> previous = new HashMap<>();
        for (String name : loggers) {
            Object logger = LoggerFactory.getLogger(name);
            previous.put(logger, levelField.getInt(logger));
            levelField.setInt(logger, level);
        }
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalErr = System.err;
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        try {
            action.run();
        } finally {
            System.err.flush();
            System.setErr(originalErr);
            for (Map.Entry<Object, Integer> entry : previous.entrySet()) {
                levelField.setInt(entry.getKey(), entry.getValue());
            }
        }
        return new String(captured.toByteArray(), StandardCharsets.UTF_8);
    }
}
