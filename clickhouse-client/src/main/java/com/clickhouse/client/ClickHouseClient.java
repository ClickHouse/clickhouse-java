package com.clickhouse.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.clickhouse.client.ClickHouseRequest.Mutation;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.config.ClickHouseBufferingMode;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseFile;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.ClickHouseWriter;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * A unified interface defines Java client for ClickHouse. A client can only
 * connect to one {@link ClickHouseNode} at a time. When switching from one node
 * to another, connection made to previous node will be closed automatically
 * before new connection being established.
 *
 * <p>
 * To decouple from concrete implementation tied to specific protocol, it is
 * recommended to use {@link #builder()} for instantiation. In order to register
 * a new type of client, please add
 * {@code META-INF/services/com.clickhouse.client.ClickHouseClient} into your
 * artifact, so that {@code java.util.SerivceLoader} can discover the
 * implementation properly in runtime.
 */
@Deprecated
public interface ClickHouseClient extends AutoCloseable {
    Logger LOG = LoggerFactory.getLogger(ClickHouseClient.class);

    /**
     * Returns a builder for creating a new client.
     *
     * @return non-null builder, which is mutable and not thread-safe
     */
    static ClickHouseClientBuilder builder() {
        return new ClickHouseClientBuilder();
    }

    /**
     * Gets default {@link java.util.concurrent.ExecutorService} for static methods
     * like {@code dump()}, {@code load()}, {@code send()}, and {@code submit()}. It
     * will be shared among all client instances when
     * {@link ClickHouseClientOption#MAX_THREADS_PER_CLIENT} is less than or equals
     * to zero.
     *
     * @return non-null default executor service
     */
    static ExecutorService getExecutorService() {
        return ClickHouseDataStreamFactory.getInstance().getExecutor();
    }

    /**
     * Gets wrapped output stream for writing data into request.
     *
     * @param config          optional configuration
     * @param output          non-null output stream
     * @param postCloseAction custom action will be performed right after closing
     *                        the output stream
     * @return wrapped output, or the same output if it's instance of
     *         {@link ClickHouseOutputStream}
     */
    static ClickHouseOutputStream getRequestOutputStream(ClickHouseConfig config, OutputStream output,
            Runnable postCloseAction) {
        if (config == null) {
            return ClickHouseOutputStream.of(output, (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue(),
                    ClickHouseCompression.NONE, ClickHouseConfig.DEFAULT_READ_COMPRESS_LEVEL, postCloseAction);
        }

        return ClickHouseOutputStream.of(output, config.getWriteBufferSize(), config.getRequestCompressAlgorithm(),
                config.getRequestCompressLevel(), postCloseAction);
    }

    /**
     * Gets piped output stream for writing data into request asynchronously. When
     * {@code config} is {@code null}, {@code config.isAsync()} is {@code false}, or
     * request buffering mode is {@link ClickHouseBufferingMode#RESOURCE_EFFICIENT},
     * this method will be same as
     * {@link #getRequestOutputStream(ClickHouseConfig, OutputStream, Runnable)}.
     *
     * @param config          optional configuration
     * @param output          non-null output stream
     * @param postCloseAction custom action will be performed right after closing
     *                        the output stream
     * @return wrapped output, or the same output if it's instance of
     *         {@link ClickHouseOutputStream}
     */
    static ClickHouseOutputStream getAsyncRequestOutputStream(ClickHouseConfig config, OutputStream output,
            Runnable postCloseAction) {
        if (config == null || !config.isAsync()
                || config.getRequestBuffering() == ClickHouseBufferingMode.RESOURCE_EFFICIENT) {
            return getRequestOutputStream(config, output, postCloseAction);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        final ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                .createPipedOutputStream(config, () -> {
                    try {
                        latch.await();

                        if (postCloseAction != null) {
                            postCloseAction.run();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Stopped waiting for writes", e);
                    }
                });
        submit(() -> {
            try (ClickHouseInputStream in = stream.getInputStream();
                    ClickHouseOutputStream out = getRequestOutputStream(config, output, postCloseAction)) {
                in.pipe(out);
            } finally {
                latch.countDown();
            }
            return null;
        });
        return stream;
    }

    /**
     * Gets wrapped input stream for reading data from response.
     *
     * @param config          optional configuration
     * @param input           non-null input stream
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    static ClickHouseInputStream getResponseInputStream(ClickHouseConfig config, InputStream input,
            Runnable postCloseAction) {
        if (config == null) {
            return ClickHouseInputStream.of(input, ClickHouseDataConfig.getDefaultReadBufferSize(),
                    ClickHouseCompression.NONE, ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL, postCloseAction);
        }

        return ClickHouseInputStream.of(input, config.getReadBufferSize(), config.getResponseCompressAlgorithm(),
                config.getResponseCompressLevel(), postCloseAction);
    }

    /**
     * Gets piped input stream for reading data from response asynchronously. When
     * {@code config} is {@code null}, {@code config.isAsync()} is {@code false}, or
     * response buffering mode is
     * {@link ClickHouseBufferingMode#RESOURCE_EFFICIENT}, this method will be same
     * as {@link #getResponseInputStream(ClickHouseConfig, InputStream, Runnable)}.
     *
     * @param config          optional configuration
     * @param input           non-null input stream
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    @SuppressWarnings("squid:S2095")
    static ClickHouseInputStream getAsyncResponseInputStream(ClickHouseConfig config, InputStream input,
            Runnable postCloseAction) {
        if (config == null || !config.isAsync()
                || config.getResponseBuffering() == ClickHouseBufferingMode.RESOURCE_EFFICIENT) {
            return getResponseInputStream(config, input, postCloseAction);
        }

        // raw response -> input
        final ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                .createPipedOutputStream(config);
        final ClickHouseInputStream wrappedInput;
        // raw response -> decompressed response -> input
        if (config.isResponseCompressed()) { // one more thread for decompression?
            final ClickHousePipedOutputStream decompressedStream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config);
            wrappedInput = getResponseInputStream(config, decompressedStream.getInputStream(), postCloseAction);
            submit(() -> {
                try (ClickHouseInputStream in = ClickHouseInputStream.of(input, config.getReadBufferSize(),
                        config.getResponseCompressAlgorithm(), config.getResponseCompressLevel(), null);
                        ClickHouseOutputStream out = decompressedStream) {
                    in.pipe(out);
                }
                return null;
            });
        } else {
            wrappedInput = getResponseInputStream(config, input, postCloseAction);
        }
        submit(() -> {
            try (ClickHouseInputStream in = wrappedInput; ClickHouseOutputStream out = stream) {
                in.pipe(out);
            }
            return null;
        });
        return stream.getInputStream();
    }

    /**
     * Runs the given task immediately in current thread. Exception will be wrapped
     * as {@link CompletionException}.
     *
     * @param <T>  return type of the task
     * @param task non-null task to run
     * @return result which may or may not be null
     * @throws CompletionException when failed to execute the task
     */
    static <T> T run(Callable<T> task) {
        try {
            return task.call();
        } catch (ClickHouseException e) {
            throw new CompletionException(e);
        } catch (CompletionException e) {
            throw e;
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof CompletionException) {
                throw (CompletionException) cause;
            } else if (cause == null) {
                cause = e;
            }
            throw new CompletionException(cause);
        }
    }

    /**
     * Runs the given task immediately in current thread. Exception will be wrapped
     * as {@link CompletionException}.
     *
     * @param task non-null task to run
     * @throws CompletionException when failed to execute the task
     */
    static void run(Runnable task) {
        try {
            task.run();
        } catch (CompletionException e) {
            throw e;
        } catch (Exception e) {
            Throwable cause = e instanceof ClickHouseException ? e : e.getCause();
            if (cause instanceof CompletionException) {
                throw (CompletionException) cause;
            } else if (cause == null) {
                cause = e;
            }
            throw new CompletionException(cause);
        }
    }

    /**
     * Submits task for execution. Depending on {@link ClickHouseDefaults#ASYNC}, it
     * may or may not use {@link #getExecutorService()} to run the task in a
     * separate thread.
     *
     * @param <T>  return type of the task
     * @param task non-null task
     * @return non-null future object to get result
     * @throws CompletionException when failed to complete the task in synchronous
     *                             mode
     */
    static <T> CompletableFuture<T> submit(Callable<T> task) {
        return (boolean) ClickHouseDefaults.ASYNC.getEffectiveDefaultValue()
                ? CompletableFuture.supplyAsync(() -> run(task), getExecutorService())
                : CompletableFuture.completedFuture(run(task));
    }

    /**
     * Submits task for execution. Depending on {@link ClickHouseDefaults#ASYNC}, it
     * may or may not use {@link #getExecutorService()} to run the task in a
     * separate thread.
     *
     * @param task non-null task
     * @return null
     * @throws CompletionException when failed to complete the task in synchronous
     *                             mode
     */
    static CompletableFuture<Void> submit(Runnable task) {
        if ((boolean) ClickHouseDefaults.ASYNC.getEffectiveDefaultValue()) {
            return CompletableFuture.runAsync(() -> run(task), getExecutorService());
        }

        run(task);
        return ClickHouseUtils.NULL_FUTURE;
    }

    /**
     * Dumps a table or query result from server into the given pass-thru stream.
     * Pass {@link com.clickhouse.data.ClickHouseFile} to dump data into a file,
     * which may or may not be compressed.
     *
     * @param server       non-null server to connect to
     * @param tableOrQuery table name or a select query
     * @param stream       non-null pass-thru stream
     * @return non-null future object to get result
     * @throws IllegalArgumentException if any of server, tableOrQuery, and output
     *                                  is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> dump(ClickHouseNode server, String tableOrQuery,
            ClickHousePassThruStream stream) {
        if (server == null || tableOrQuery == null || stream == null) {
            throw new IllegalArgumentException("Non-null server, tableOrQuery, and pass-thru stream are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        final String theQuery = tableOrQuery.trim();

        return submit(() -> {
            try (ClickHouseClient client = newInstance(theServer.getProtocol())) {
                ClickHouseRequest<?> request = client.read(theServer).output(stream);
                // FIXME what if the table name is `try me`?
                if (theQuery.indexOf(' ') < 0) {
                    request.table(theQuery);
                } else {
                    request.query(theQuery);
                }

                try (ClickHouseResponse response = request.executeAndWait()) {
                    return response.getSummary();
                }
            }
        });
    }

    /**
     * Dumps a table or query result from server into a file. File will be
     * created/overwrited as needed.
     *
     * @param server       non-null server to connect to
     * @param tableOrQuery table name or a select query
     * @param file         output file
     * @param compression  compression algorithm to use
     * @param format       output format to use
     * @return non-null future object to get result
     * @throws IllegalArgumentException if any of server, tableOrQuery, and output
     *                                  is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> dump(ClickHouseNode server, String tableOrQuery, String file,
            ClickHouseCompression compression, ClickHouseFormat format) {
        return dump(server, tableOrQuery, ClickHouseFile.of(file, compression, format));
    }

    /**
     * Dumps a table or query result from server into output stream.
     *
     * @param server       non-null server to connect to
     * @param tableOrQuery table name or a select query
     * @param format       output format to use, null means
     *                     {@link ClickHouseFormat#TabSeparated}
     * @param compression  compression algorithm to use, null means
     *                     {@link ClickHouseCompression#NONE}
     * @param output       output stream, which will be closed automatically at the
     *                     end of the call
     * @return future object to get result
     * @throws IllegalArgumentException if any of server, tableOrQuery, and output
     *                                  is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> dump(ClickHouseNode server, String tableOrQuery,
            OutputStream output, ClickHouseCompression compression, ClickHouseFormat format) {
        if (server == null || tableOrQuery == null || output == null) {
            throw new IllegalArgumentException("Non-null server, tableOrQuery, and output are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        final String theQuery = tableOrQuery.trim();

        return submit(() -> {
            try (ClickHouseClient client = newInstance(theServer.getProtocol())) {
                ClickHouseRequest<?> request = client.read(theServer).compressServerResponse(compression)
                        .format(format).output(output);
                // FIXME what if the table name is `try me`?
                if (theQuery.indexOf(' ') < 0) {
                    request.table(theQuery);
                } else {
                    request.query(theQuery);
                }

                try (ClickHouseResponse response = request.executeAndWait()) {
                    return response.getSummary();
                }
            } finally {
                try {
                    output.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        });
    }

    /**
     * Loads data from the given pass-thru stream into a table. Pass
     * {@link com.clickhouse.data.ClickHouseFile} to load data from a file, which
     * may or may not be compressed.
     *
     * @param server non-null server to connect to
     * @param table  non-null target table
     * @param stream non-null pass-thru stream
     * @return future object to get result
     * @throws IllegalArgumentException if any of server, table, and input is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> load(ClickHouseNode server, String table,
            ClickHousePassThruStream stream) {
        if (server == null || table == null || stream == null) {
            throw new IllegalArgumentException("Non-null server, table, and pass-thru stream are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        return submit(() -> {
            try (ClickHouseClient client = newInstance(theServer.getProtocol());
                    ClickHouseResponse response = client.write(theServer).table(table).data(stream)
                            .executeAndWait()) {
                return response.getSummary();
            }
        });
    }

    /**
     * Loads data from a customer writer into table using specified format and
     * compression algorithm.
     *
     * @param server      non-null server to connect to
     * @param table       non-null target table
     * @param writer      non-null custom writer to produce data
     * @param compression compression algorithm to use
     * @param format      input format to use
     * @return future object to get result
     * @throws IllegalArgumentException if any of server, table, and input is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> load(ClickHouseNode server, String table,
            ClickHouseWriter writer, ClickHouseCompression compression, ClickHouseFormat format) {
        if (server == null || table == null || writer == null) {
            throw new IllegalArgumentException("Non-null server, table, and custom writer are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        return submit(() -> {
            try (ClickHouseClient client = newInstance(theServer.getProtocol());
                    ClickHouseResponse response = client.write(theServer).table(table).data(writer)
                            .decompressClientRequest(compression).format(format).executeAndWait()) {
                return response.getSummary();
            }
        });
    }

    /**
     * Loads data from a file into table using specified format and compression
     * algorithm. Same as
     * {@code load(server, table, ClickHouseFile.of(file, compression, format))}
     *
     * @param server      non-null server to connect to
     * @param table       non-null target table
     * @param file        file to load
     * @param compression compression algorithm to use
     * @param format      input format to use
     * @return future object to get result
     * @throws IllegalArgumentException if any of server, table, and input is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> load(ClickHouseNode server, String table,
            String file, ClickHouseCompression compression, ClickHouseFormat format) {
        return load(server, table, ClickHouseFile.of(file, compression, format));
    }

    /**
     * Loads data from input stream into a table using specified format and
     * compression algorithm.
     *
     * @param server      non-null server to connect to
     * @param table       non-null target table
     * @param input       input stream, which will be closed automatically at the
     *                    end of the call
     * @param compression compression algorithm to use
     * @param format      input format to use
     * @return future object to get result
     * @throws IllegalArgumentException if any of server, table, and input is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> load(ClickHouseNode server, String table, InputStream input,
            ClickHouseCompression compression, ClickHouseFormat format) {
        if (server == null || table == null || input == null) {
            throw new IllegalArgumentException("Non-null server, table, and input are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        return submit(() -> {
            try (ClickHouseClient client = newInstance(theServer.getProtocol());
                    ClickHouseResponse response = client.write(theServer).table(table).data(input)
                            .decompressClientRequest(compression).format(format).executeAndWait()) {
                return response.getSummary();
            } finally {
                try {
                    input.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        });
    }

    /**
     * Creates a new instance compatible with any of the given protocols.
     *
     * @param preferredProtocols preferred protocols
     * @return new instance compatible with any of the given protocols
     */
    static ClickHouseClient newInstance(ClickHouseProtocol... preferredProtocols) {
        return builder().nodeSelector(ClickHouseNodeSelector.of(null, preferredProtocols)).build();
    }

    /**
     * Creates a new instance with default credentials compatible with any of the
     * given protocols.
     *
     * @param defaultCredentials default credentials
     * @param preferredProtocols preferred protocols
     * @return new instance compatible with any of the given protocols using default
     *         credentials
     */
    static ClickHouseClient newInstance(ClickHouseCredentials defaultCredentials,
            ClickHouseProtocol... preferredProtocols) {
        return builder()
                .nodeSelector(ClickHouseNodeSelector.of(null, preferredProtocols))
                .defaultCredentials(defaultCredentials)
                .build();
    }

    /**
     * Sends one or more SQL queries to specified server, and execute them one by
     * one. Session will be created automatically if there's more than one SQL
     * query.
     *
     * @param server non-null server to connect to
     * @param sql    non-null SQL query
     * @param more   more SQL queries if any
     * @return list of {@link ClickHouseResponseSummary} one for each execution
     * @throws IllegalArgumentException if server or sql is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<List<ClickHouseResponseSummary>> send(ClickHouseNode server, String sql, String... more) {
        if (server == null || sql == null || more == null) {
            throw new IllegalArgumentException("Non-null server and sql are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        List<String> queries = new ArrayList<>(more.length + 1);
        if (!ClickHouseChecker.isNullOrBlank(sql)) {
            queries.add(sql);
        }
        for (String query : more) {
            if (!ClickHouseChecker.isNullOrBlank(query)) {
                queries.add(query);
            }
        }
        if (queries.isEmpty()) {
            throw new IllegalArgumentException("At least one non-blank query is required");
        }

        return submit(() -> {
            List<ClickHouseResponseSummary> list = new ArrayList<>(queries.size());

            // set async to false so that we don't have to create additional thread
            try (ClickHouseClient client = ClickHouseClient.builder()
                    .nodeSelector(ClickHouseNodeSelector.of(theServer.getProtocol()))
                    .option(ClickHouseClientOption.ASYNC, false).build()) {
                ClickHouseRequest<?> request = client.read(theServer).format(ClickHouseFormat.RowBinary);
                if ((boolean) ClickHouseDefaults.AUTO_SESSION.getEffectiveDefaultValue() && queries.size() > 1) {
                    request.session(request.getManager().createSessionId(), false);
                }
                for (String query : queries) {
                    try (ClickHouseResponse resp = request.query(query).executeAndWait()) {
                        list.add(resp.getSummary());
                    }
                }
            }

            return list;
        });
    }

    /**
     * Sends SQL query along with stringified parameters to specified server.
     *
     * @param server non-null server to connect to
     * @param sql    non-null SQL query
     * @param params non-null stringified parameters
     * @return list of {@link ClickHouseResponseSummary} one for each execution
     * @throws IllegalArgumentException if any of server, sql, and params is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> send(ClickHouseNode server, String sql,
            Map<String, String> params) {
        if (server == null || sql == null || params == null) {
            throw new IllegalArgumentException("Non-null server, sql and parameters are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        return submit(() -> {
            // set async to false so that we don't have to create additional thread
            try (ClickHouseClient client = ClickHouseClient.builder()
                    .nodeSelector(ClickHouseNodeSelector.of(theServer.getProtocol()))
                    .option(ClickHouseClientOption.ASYNC, false).build();
                    ClickHouseResponse resp = client.read(theServer).format(ClickHouseFormat.RowBinary).query(sql)
                            .params(params).executeAndWait()) {
                return resp.getSummary();
            }
        });
    }

    /**
     * Sends SQL query along with raw parameters(e.g. byte value for Int8) to
     * specified server. Parameters will be stringified based on given column types.
     *
     * @param server  non-null server to connect to
     * @param sql     non-null SQL query
     * @param columns non-empty column list
     * @param params  non-empty raw parameters
     * @return list of {@link ClickHouseResponseSummary} one for each execution
     * @throws IllegalArgumentException if columns is null, empty or contains null
     *                                  column
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<List<ClickHouseResponseSummary>> send(ClickHouseNode server, String sql,
            List<ClickHouseColumn> columns, Object[]... params) {
        int len = columns == null ? 0 : columns.size();
        if (len == 0) {
            throw new IllegalArgumentException("Non-empty column list is required");
        }

        // FIXME better get the configuration from request/client
        ClickHouseConfig config = new ClickHouseConfig();
        ClickHouseValue[] templates = new ClickHouseValue[len];
        int index = 0;
        for (ClickHouseColumn column : columns) {
            templates[index++] = ClickHouseChecker.nonNull(column, ClickHouseColumn.TYPE_NAME).newValue(config);
        }

        return send(server, sql, templates, params);
    }

    /**
     * Sends SQL query along with template objects and raw parameters to specified
     * server.
     *
     * @param server    non-null server to connect to
     * @param sql       non-null SQL query
     * @param templates non-empty template objects to stringify parameters
     * @param params    non-empty raw parameters
     * @return list of {@link ClickHouseResponseSummary} one for each execution
     * @throws IllegalArgumentException if no named parameter in the query, or
     *                                  templates or params is null or empty
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<List<ClickHouseResponseSummary>> send(ClickHouseNode server, String sql,
            ClickHouseValue[] templates, Object[]... params) {
        int len = templates != null ? templates.length : 0;
        int size = params != null ? params.length : 0;
        if (len == 0 || size == 0) {
            throw new IllegalArgumentException("Non-empty templates and parameters are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        return submit(() -> {
            List<ClickHouseResponseSummary> list = new ArrayList<>(size);

            // set async to false so that we don't have to create additional thread
            try (ClickHouseClient client = ClickHouseClient.builder()
                    .nodeSelector(ClickHouseNodeSelector.of(theServer.getProtocol()))
                    .option(ClickHouseClientOption.ASYNC, false).build()) {
                // format doesn't matter here as we only need a summary
                ClickHouseRequest<?> request = client.read(theServer).format(ClickHouseFormat.RowBinary).query(sql);
                for (int i = 0; i < size; i++) {
                    Object[] o = params[i];
                    String[] arr = new String[len];
                    for (int j = 0, olen = o == null ? 0 : o.length; j < len; j++) {
                        ClickHouseValue v = templates[j];
                        if (j < olen) {
                            arr[j] = v != null ? v.update(o[j]).toSqlExpression()
                                    : ClickHouseValues.convertToSqlExpression(o[j]);
                        } else {
                            arr[j] = v != null ? v.resetToNullOrEmpty().toSqlExpression() : ClickHouseValues.NULL_EXPR;
                        }
                    }
                    try (ClickHouseResponse resp = request.params(arr).executeAndWait()) {
                        list.add(resp.getSummary());
                    }
                }
            }

            return list;
        });
    }

    /**
     * Sends SQL query along with stringified parameters to specified server.
     *
     * @param server non-null server to connect to
     * @param sql    non-null SQL query
     * @param params non-null stringified parameters
     * @return list of {@link ClickHouseResponseSummary} one for each execution
     * @throws IllegalArgumentException if any of server, sql, or params is null; or
     *                                  no named parameter in the query
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<List<ClickHouseResponseSummary>> send(ClickHouseNode server, String sql,
            String[][] params) {
        if (server == null || sql == null || params == null) {
            throw new IllegalArgumentException("Non-null server, sql, and parameters are required");
        } else if (params.length == 0) {
            return send(server, sql);
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        return submit(() -> {
            List<ClickHouseResponseSummary> list = new ArrayList<>(params.length);

            // set async to false so that we don't have to create additional thread
            try (ClickHouseClient client = ClickHouseClient.builder()
                    .nodeSelector(ClickHouseNodeSelector.of(theServer.getProtocol()))
                    .option(ClickHouseClientOption.ASYNC, false).build()) {
                // format doesn't matter here as we only need a summary
                ClickHouseRequest<?> request = client.read(theServer).format(ClickHouseFormat.RowBinary);
                ClickHouseParameterizedQuery query = ClickHouseParameterizedQuery.of(request.getConfig(), sql);
                StringBuilder builder = new StringBuilder();
                for (String[] p : params) {
                    builder.setLength(0);
                    query.apply(builder, p);
                    try (ClickHouseResponse resp = request.query(builder.toString()).executeAndWait()) {
                        list.add(resp.getSummary());
                    }
                }
            }

            return list;
        });
    }

    /**
     * Tests whether the given protocol is supported or not. An advanced client can
     * support as many protocols as needed.
     *
     * @param protocol protocol to test, null is treated as
     *                 {@link ClickHouseProtocol#ANY}
     * @return true if the given protocol is {@link ClickHouseProtocol#ANY} or
     *         supported by this client; false otherwise
     */
    default boolean accept(ClickHouseProtocol protocol) {
        return protocol == null || protocol == ClickHouseProtocol.ANY;
    }

    /**
     * Connects to one or more ClickHouse servers to read. Same as
     * {@code read(ClickHouseNodes.of(uri))}.
     *
     * @param endpoints non-empty connection string separated by comma
     * @return non-null request object holding references to this client and node
     *         provider
     */
    default ClickHouseRequest<?> read(String endpoints) {
        return read(ClickHouseNodes.of(endpoints));
    }

    /**
     * Connects to a list of managed ClickHouse servers to read.
     *
     * @param nodes non-null list of servers for read
     * @return non-null request object holding references to this client and node
     *         provider
     */
    default ClickHouseRequest<?> read(ClickHouseNodes nodes) {
        return read(nodes, nodes.template.config.getAllOptions());
    }

    /**
     * Connects to a ClickHouse server to read.
     *
     * @param node non-null server for read
     * @return non-null request object holding references to this client and server
     */
    default ClickHouseRequest<?> read(ClickHouseNode node) {
        return read(node, node.config.getAllOptions());
    }

    /**
     * Connects to a ClickHouse server to read.
     *
     * @param nodeFunc function to get a {@link ClickHouseNode} to connect to
     * @param options  optional client options for connecting to the server
     * @return non-null request object holding references to this client and server
     */
    default ClickHouseRequest<?> read(Function<ClickHouseNodeSelector, ClickHouseNode> nodeFunc,
            Map<ClickHouseOption, Serializable> options) {
        return new ClickHouseRequest<>(this, ClickHouseChecker.nonNull(nodeFunc, "Node"), null, options, false);
    }

    /**
     * Writes into a ClickHouse server.
     *
     * @param node non-null server for write
     * @return non-null request object holding references to this client and server
     */
    default Mutation write(ClickHouseNode node) {
        return read(node).write();
    }

    /**
     * Creates an immutable copy of the request if it's not sealed, and sends it to
     * a node hold by the request(e.g. {@link ClickHouseNode} returned from
     * {@code request.getServer()}). Connection will be made for the first-time
     * invocation, and then it will be reused in subsequential calls to the extract
     * same {@link ClickHouseNode} until {@link #close()} is invoked.
     *
     * @param request request object which will be sealed(immutable copy) upon
     *                execution, meaning you're free to make any change to this
     *                object(e.g. prepare for next call using different SQL
     *                statement) without impacting the execution
     * @return non-null future object to get result
     * @throws CompletionException when error occurred during execution
     */
    CompletableFuture<ClickHouseResponse> execute(ClickHouseRequest<?> request);

    /**
     * Synchronous version of {@link #execute(ClickHouseRequest)}.
     *
     * @param request request object which will be sealed(immutable copy) upon
     *                execution
     * @return non-null response
     * @throws ClickHouseException when error occurred during execution
     */
    default ClickHouseResponse executeAndWait(ClickHouseRequest<?> request) throws ClickHouseException {
        final ClickHouseRequest<?> sealedRequest = request.seal();

        try {
            return execute(sealedRequest).get(sealedRequest.getConfig().getSocketTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ClickHouseException.forCancellation(e, sealedRequest.getServer());
        } catch (CancellationException e) {
            throw ClickHouseException.forCancellation(e, sealedRequest.getServer());
        } catch (CompletionException | ExecutionException | TimeoutException | UncheckedIOException e) {
            Throwable cause = e.getCause();
            if (cause == null) {
                cause = e;
            }
            throw cause instanceof ClickHouseException ? (ClickHouseException) cause
                    : ClickHouseException.of(cause, sealedRequest.getServer());
        } catch (RuntimeException e) { // unexpected
            throw ClickHouseException.of(e, sealedRequest.getServer());
        }
    }

    /**
     * Gets the immutable configuration associated with this client. In most cases
     * it's the exact same one passed to {@link #init(ClickHouseConfig)} method for
     * initialization.
     *
     * @return non-null configuration associated with this client
     */
    ClickHouseConfig getConfig();

    /**
     * Gets class defining client-specific options.
     *
     * @return class defining client-specific options, null means no specific option
     */
    default Class<? extends ClickHouseOption> getOptionClass() {
        return null;
    }

    /**
     * Initializes the client using immutable configuration extracted from the
     * builder using {@link ClickHouseClientBuilder#getConfig()}. In general, it's
     * {@link ClickHouseClientBuilder}'s responsiblity to call this method to
     * initialize the client at the end of {@link ClickHouseClientBuilder#build()}.
     * However, sometimes, you may want to call this method explicitly in order to
     * (re)initialize the client based on certain needs. If that's the case, please
     * consider the environment when calling this method to avoid concurrent
     * modification, and keep in mind that 1) ClickHouseConfig is immutable but
     * ClickHouseClient is NOT; and 2) no guarantee that this method is thread-safe.
     *
     * @param config immutable configuration extracted from the builder
     */
    default void init(ClickHouseConfig config) {
        ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME);
    }

    /**
     * Tests if the given server is alive or not. It always returns {@code false}
     * when server is {@code null} or timeout is less than one. Pay attention that
     * it's a synchronous call with minimum overhead(e.g. tiny buffer, no
     * compression and no deserialization etc).
     *
     * @param server  non-null server to test
     * @param timeout timeout in millisecond, should be greater than zero
     * @return true if the server is alive; false otherwise
     */
    default boolean ping(ClickHouseNode server, int timeout) {
        if (server != null) {
            if (timeout < 1) {
                timeout = server.config.getConnectionTimeout();
            }
            if (server.getProtocol() == ClickHouseProtocol.ANY) {
                server = ClickHouseNode.probe(server.getHost(), server.getPort(), timeout);
            }
            // Request to this specific ClickHouse node
            ClickHouseRequest<?> request = new ClickHouseRequest<>(this, server, new AtomicReference<>(server), server.config.getAllOptions(), false);
            try (ClickHouseResponse resp = request
                    .option(ClickHouseClientOption.ASYNC, false) // use current thread
                    .option(ClickHouseClientOption.CONNECTION_TIMEOUT, timeout)
                    .option(ClickHouseClientOption.SOCKET_TIMEOUT, timeout)
                    .option(ClickHouseClientOption.MAX_QUEUED_BUFFERS, 1) // enough with only one buffer
                    .format(ClickHouseFormat.TabSeparated)
                    .query("SELECT 1 FORMAT TabSeparated").execute()
                    .get(timeout, TimeUnit.MILLISECONDS)) {
                return resp != null;
            } catch (Exception e) {
                LOG.debug("Failed to connect to the server", e);
                return false;
            }
        }

        return false;
    }

    @Override
    void close();
}
