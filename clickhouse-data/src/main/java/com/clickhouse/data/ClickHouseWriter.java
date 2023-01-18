package com.clickhouse.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.clickhouse.config.ClickHouseBufferingMode;

@FunctionalInterface
public interface ClickHouseWriter {
    static void writeAndClose(ClickHouseDataConfig config, ClickHouseWriter writer, ClickHouseOutputStream output)
            throws IOException {
        if (config.isAsync() || config.getWriteBufferingMode() == ClickHouseBufferingMode.PERFORMANCE) {
            CompletableFuture.supplyAsync(() -> {
                try (ClickHouseOutputStream out = output) {
                    writer.write(output);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return null;
            });
        } else {
            try (ClickHouseOutputStream out = output) {
                writer.write(output);
            }
        }
    }

    /**
     * Writes value to output stream.
     *
     * @param output non-null output stream
     * @throws IOException when failed to write data to output stream
     */
    void write(ClickHouseOutputStream output) throws IOException;
}
