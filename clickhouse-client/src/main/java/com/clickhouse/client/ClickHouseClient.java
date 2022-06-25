package com.clickhouse.client;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.clickhouse.client.config.ClickHouseBufferingMode;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseOption;

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
public interface ClickHouseClient extends AutoCloseable {

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
        return ClickHouseClientBuilder.defaultExecutor;
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
                    ClickHouseCompression.NONE, postCloseAction);
        }

        return ClickHouseOutputStream.of(output, config.getWriteBufferSize(), config.getRequestCompressAlgorithm(),
                postCloseAction);
    }

    /**
     * Gets piped output stream for writing data into request asynchronously. When
     * {@code config} is null or {@code config.isAsync()} is false, this method is
     * same as
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
            return ClickHouseInputStream.of(input, (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue(),
                    ClickHouseCompression.NONE, postCloseAction);
        }

        return ClickHouseInputStream.of(input, config.getReadBufferSize(), config.getResponseCompressAlgorithm(),
                postCloseAction);
    }

    /**
     * Gets piped input stream for reading data from response asynchronously. When
     * {@code config} is null or {@code config.isAsync()} is faluse, this method is
     * same as
     * {@link #getResponseInputStream(ClickHouseConfig, InputStream, Runnable)}.
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
                .createPipedOutputStream(config, null);
        final ClickHouseInputStream wrappedInput;
        // raw response -> decompressed response -> input
        if (config.isResponseCompressed()) { // one more thread for decompression?
            final ClickHousePipedOutputStream decompressedStream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config, null);
            wrappedInput = getResponseInputStream(config, decompressedStream.getInputStream(), postCloseAction);
            submit(() -> {
                try (ClickHouseInputStream in = ClickHouseInputStream.of(input, config.getReadBufferSize(),
                        config.getResponseCompressAlgorithm(), null); ClickHouseOutputStream out = decompressedStream) {
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
     * Submits task for execution. Depending on {@link ClickHouseDefaults#ASYNC}, it
     * may or may not use {@link #getExecutorService()} to run the task in a
     * separate thread.
     * 
     * @param <T>  return type of the task
     * @param task non-null task
     * @return non-null future object to get result
     * @throws CompletionException when failed to complete the task
     */
    static <T> CompletableFuture<T> submit(Callable<T> task) {
        try {
            return (boolean) ClickHouseDefaults.ASYNC.getEffectiveDefaultValue() ? CompletableFuture.supplyAsync(() -> {
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
            }, getExecutorService()) : CompletableFuture.completedFuture(task.call());
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
     * Dumps a table or query result from server into a file. File will be
     * created/overwrited as needed.
     *
     * @param server       non-null server to connect to
     * @param tableOrQuery table name or a select query
     * @param file         output file
     * @return non-null future object to get result
     * @throws IllegalArgumentException if any of server, tableOrQuery, and output
     *                                  is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> dump(ClickHouseNode server, String tableOrQuery,
            ClickHouseFile file) {
        if (server == null || tableOrQuery == null || file == null) {
            throw new IllegalArgumentException("Non-null server, tableOrQuery, and file are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        final String theQuery = tableOrQuery.trim();

        return submit(() -> {
            try (ClickHouseClient client = newInstance(theServer.getProtocol())) {
                ClickHouseRequest<?> request = client.connect(theServer).output(file);
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
     * @param format       output format to use
     * @param compression  compression algorithm to use
     * @param file         output file
     * @return non-null future object to get result
     * @throws IllegalArgumentException if any of server, tableOrQuery, and output
     *                                  is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> dump(ClickHouseNode server, String tableOrQuery,
            ClickHouseFormat format, ClickHouseCompression compression, String file) {
        return dump(server, tableOrQuery, ClickHouseFile.of(file, compression, 0, format));
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
            ClickHouseFormat format, ClickHouseCompression compression, OutputStream output) {
        if (server == null || tableOrQuery == null || output == null) {
            throw new IllegalArgumentException("Non-null server, tableOrQuery, and output are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        final String theQuery = tableOrQuery.trim();

        return submit(() -> {
            try (ClickHouseClient client = newInstance(theServer.getProtocol())) {
                ClickHouseRequest<?> request = client.connect(theServer).compressServerResponse(compression)
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
                } catch (Exception e) {
                    // ignore
                }
            }
        });
    }

    /**
     * Loads data from given file into a table.
     *
     * @param server non-null server to connect to
     * @param table  non-null target table
     * @param file   non-null file
     * @return future object to get result
     * @throws IllegalArgumentException if any of server, table, and input is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> load(ClickHouseNode server, String table, ClickHouseFile file) {
        if (server == null || table == null || file == null) {
            throw new IllegalArgumentException("Non-null server, table, and file are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        return submit(() -> {
            try (ClickHouseClient client = newInstance(theServer.getProtocol());
                    ClickHouseResponse response = client.connect(theServer).write().table(table).data(file)
                            .executeAndWait()) {
                return response.getSummary();
            }
        });
    }

    /**
     * Loads data from a file into table using specified format and compression
     * algorithm.
     *
     * @param server      non-null server to connect to
     * @param table       non-null target table
     * @param format      input format to use
     * @param compression compression algorithm to use
     * @param file        file to load
     * @return future object to get result
     * @throws IllegalArgumentException if any of server, table, and input is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> load(ClickHouseNode server, String table,
            ClickHouseFormat format, ClickHouseCompression compression, String file) {
        return load(server, table, ClickHouseFile.of(file, compression, 0, format));
    }

    /**
     * Loads data from a custom writer into a table using specified format and
     * compression algorithm.
     *
     * @param server      non-null server to connect to
     * @param table       non-null target table
     * @param format      input format to use
     * @param compression compression algorithm to use
     * @param writer      non-null custom writer to generate data
     * @return future object to get result
     * @throws IllegalArgumentException if any of server, table, and writer is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> load(ClickHouseNode server, String table,
            ClickHouseFormat format, ClickHouseCompression compression, ClickHouseWriter writer) {
        if (server == null || table == null || writer == null) {
            throw new IllegalArgumentException("Non-null server, table, and writer are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        return submit(() -> {
            InputStream input = null;
            // must run in async mode so that we won't hold everything in memory
            try (ClickHouseClient client = ClickHouseClient.builder()
                    .nodeSelector(ClickHouseNodeSelector.of(theServer.getProtocol()))
                    .option(ClickHouseClientOption.ASYNC, true).build()) {
                ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                        .createPipedOutputStream(client.getConfig(), null);
                // execute query in a separate thread(because async is explicitly set to true)
                CompletableFuture<ClickHouseResponse> future = client.connect(theServer).write().table(table)
                        .decompressClientRequest(compression).format(format).data(input = stream.getInputStream())
                        .execute();
                try {
                    // write data into stream in current thread
                    writer.write(stream);
                } finally {
                    stream.close();
                }
                // wait until write & read acomplished
                try (ClickHouseResponse response = future.get()) {
                    return response.getSummary();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (CancellationException e) {
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (ExecutionException e) {
                throw ClickHouseException.of(e, theServer);
            } finally {
                if (input != null) {
                    try {
                        input.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        });
    }

    /**
     * Loads data from input stream into a table using specified format and
     * compression algorithm.
     *
     * @param server      non-null server to connect to
     * @param table       non-null target table
     * @param format      input format to use
     * @param compression compression algorithm to use
     * @param input       input stream, which will be closed automatically at the
     *                    end of the call
     * @return future object to get result
     * @throws IllegalArgumentException if any of server, table, and input is null
     * @throws CompletionException      when error occurred during execution
     */
    static CompletableFuture<ClickHouseResponseSummary> load(ClickHouseNode server, String table,
            ClickHouseFormat format, ClickHouseCompression compression, InputStream input) {
        if (server == null || table == null || input == null) {
            throw new IllegalArgumentException("Non-null server, table, and input are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        return submit(() -> {
            try (ClickHouseClient client = newInstance(theServer.getProtocol());
                    ClickHouseResponse response = client.connect(theServer).write().table(table)
                            .decompressClientRequest(compression).format(format).data(input).executeAndWait()) {
                return response.getSummary();
            } finally {
                try {
                    input.close();
                } catch (Exception e) {
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
        if (server == null || sql == null) {
            throw new IllegalArgumentException("Non-null server and sql are required");
        }

        // in case the protocol is ANY
        final ClickHouseNode theServer = server.probe();

        List<String> queries = new LinkedList<>();
        queries.add(sql);
        if (more != null && more.length > 0) {
            for (String query : more) {
                // dedup?
                queries.add(ClickHouseChecker.nonNull(query, "query"));
            }
        }

        return submit(() -> {
            List<ClickHouseResponseSummary> list = new LinkedList<>();

            // set async to false so that we don't have to create additional thread
            try (ClickHouseClient client = ClickHouseClient.builder()
                    .nodeSelector(ClickHouseNodeSelector.of(theServer.getProtocol()))
                    .option(ClickHouseClientOption.ASYNC, false).build()) {
                ClickHouseRequest<?> request = client.connect(theServer).format(ClickHouseFormat.RowBinary);
                if ((boolean) ClickHouseDefaults.AUTO_SESSION.getEffectiveDefaultValue() && queries.size() > 1) {
                    request.session(UUID.randomUUID().toString(), false);
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
                    ClickHouseResponse resp = client.connect(theServer).format(ClickHouseFormat.RowBinary).query(sql)
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
            templates[index++] = ClickHouseValues.newValue(config, ClickHouseChecker.nonNull(column, "column"));
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
                ClickHouseRequest<?> request = client.connect(theServer).format(ClickHouseFormat.RowBinary).query(sql);
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
                ClickHouseRequest<?> request = client.connect(theServer).format(ClickHouseFormat.RowBinary);
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
     * Connects to one or more ClickHouse servers. Same as
     * {@code connect(ClickHouseNodes.of(uri))}.
     *
     * @param enpoints non-empty URIs separated by comman
     * @return non-null request object holding references to this client and node
     *         provider
     */
    default ClickHouseRequest<?> connect(String enpoints) {
        return connect(ClickHouseNodes.of(enpoints));
    }

    /**
     * Connects to a list of managed ClickHouse servers.
     *
     * @param nodes non-null list of servers to connect to
     * @return non-null request object holding references to this client and node
     *         provider
     */
    default ClickHouseRequest<?> connect(ClickHouseNodes nodes) {
        return new ClickHouseRequest<>(this, ClickHouseChecker.nonNull(nodes, "Nodes"),
                null, nodes.template.config.getAllOptions(), false);
    }

    /**
     * Connects to a ClickHouse server.
     *
     * @param node non-null server to connect to
     * @return non-null request object holding references to this client and node
     *         provider
     */
    default ClickHouseRequest<?> connect(ClickHouseNode node) {
        return new ClickHouseRequest<>(this, ClickHouseChecker.nonNull(node, "Node"), null, node.config.getAllOptions(),
                false);
    }

    /**
     * Connects to a ClickHouse server defined by the given
     * {@link java.util.function.Function}. You can pass either
     * {@link ClickHouseCluster}, {@link ClickHouseNodes} or {@link ClickHouseNode}
     * here, as all of them implemented the same interface.
     *
     * <p>
     * Please be aware that this is nothing but an intention, so no network
     * communication happens until {@link #execute(ClickHouseRequest)} is
     * invoked(usually triggered by {@code request.execute()}).
     *
     * @param nodeFunc function to get a {@link ClickHouseNode} to connect to
     * @return non-null request object holding references to this client and node
     *         provider
     */
    default ClickHouseRequest<?> connect(Function<ClickHouseNodeSelector, ClickHouseNode> nodeFunc) {
        return new ClickHouseRequest<>(this, ClickHouseChecker.nonNull(nodeFunc, "nodeFunc"), null, null, false);
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
            return execute(sealedRequest).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ClickHouseException.forCancellation(e, sealedRequest.getServer());
        } catch (CancellationException e) {
            throw ClickHouseException.forCancellation(e, sealedRequest.getServer());
        } catch (CompletionException | ExecutionException | UncheckedIOException e) {
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
        ClickHouseChecker.nonNull(config, "configuration");
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
            try (ClickHouseResponse resp = connect(server) // create request
                    .option(ClickHouseClientOption.ASYNC, false) // use current thread
                    .option(ClickHouseClientOption.CONNECTION_TIMEOUT, timeout)
                    .option(ClickHouseClientOption.SOCKET_TIMEOUT, timeout)
                    .option(ClickHouseClientOption.BUFFER_SIZE, 8) // actually 4 bytes should be enough
                    .option(ClickHouseClientOption.MAX_QUEUED_BUFFERS, 1) // enough with only one buffer
                    .format(ClickHouseFormat.TabSeparated)
                    .query("SELECT 1 FORMAT TabSeparated").execute()
                    .get(timeout, TimeUnit.MILLISECONDS)) {
                return resp != null;
            } catch (Exception e) {
                // ignore
            }
        }

        return false;
    }

    @Override
    void close();
}
