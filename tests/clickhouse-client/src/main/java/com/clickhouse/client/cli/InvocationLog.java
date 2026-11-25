package com.clickhouse.client.cli;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Collects troubleshooting lines for one invocation and appends them to the file named by
 * {@code CLICKHOUSE_CLIENT_CLI_LOG}. Without that variable, it does nothing. The lines are written in one write, so
 * the lines of parallel invocations do not mix.
 */
final class InvocationLog {
    static final String LOG_PATH_ENV = "CLICKHOUSE_CLIENT_CLI_LOG";

    private final Path path;
    private final StringBuilder lines = new StringBuilder();

    InvocationLog(String path) {
        this.path = path == null || path.isEmpty() ? null : Paths.get(path);
        add("=== clickhouse-client invocation", Instant.now().toString());
    }

    void add(String key, String value) {
        if (path == null) {
            return;
        }
        lines.append(key).append('=').append(String.valueOf(value).replace("\n", "\\n")).append('\n');
    }

    /**
     * Appends the lines to the log file. A log that cannot be written must not change the result of a test, so a
     * write error is ignored here; the missing lines in the log show it.
     */
    void flush() {
        if (path == null) {
            return;
        }
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND)) {
                channel.write(ByteBuffer.wrap(lines.toString().getBytes(StandardCharsets.UTF_8)));
            }
        } catch (Exception ignored) {
            // See the method comment.
        }
    }
}
