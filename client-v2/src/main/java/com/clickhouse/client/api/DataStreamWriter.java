package com.clickhouse.client.api;

import java.io.IOException;
import java.io.OutputStream;

public interface DataStreamWriter {

    /**
     * Called by client when output stream is ready for user data.
     * This method is called once per operation, so all data should be written while the call.
     * Output stream will be closed by client.
     * When client compression is enabled, then output stream will be a compressing one.
     * If {@link ClientConfigProperties#APP_COMPRESSED_DATA} is set for an operation,
     * then {@param out} will be raw IO stream without compression.
     * @param out - output stream
     * @throws IOException - when any IO exceptions happens.
     */
    void onOutput(OutputStream out) throws IOException;

    /**
     * Is called when client is going to perform a retry.
     * It is optional to implement this method because most cases there is nothing to reset.
     * Useful to reset wrapped stream or throw exception to indicate that retry is not supported for a data source.
     * @throws IOException - when any IO exception happens.
     */
    default void onRetry() throws IOException {}
}
