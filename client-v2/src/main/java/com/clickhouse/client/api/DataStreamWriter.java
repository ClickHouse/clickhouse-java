package com.clickhouse.client.api;

import java.io.IOException;
import java.io.OutputStream;

public interface DataStreamWriter {

    /**
     * Called by client when output stream is ready for user data.
     * This method is called once for each request that the client sends. Write all data in this call.
     * A retry sends a new request, so the method is called again and must write all data again.
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
     * The client calls this method before each retry, also when the failed attempt did not call
     * {@link #onOutput(OutputStream)}.
     * It is optional to implement this method because most cases there is nothing to reset.
     * Useful to reset wrapped stream or throw exception to indicate that retry is not supported for a data source.
     * If this method throws an {@link IOException}, the operation fails with a {@link ClientException} and the
     * client makes no more attempts.
     * @throws IOException - when any IO exception happens.
     */
    default void onRetry() throws IOException {}
}
