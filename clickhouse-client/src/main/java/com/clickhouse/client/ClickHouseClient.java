package com.clickhouse.client;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.data.ClickHousePipedStream;

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
     * {@link com.clickhouse.client.config.ClickHouseClientOption#MAX_THREADS_PER_CLIENT}
     * is less than or equals to zero.
     * 
     * @return non-null default executor service
     */
    static ExecutorService getExecutorService() {
        return ClickHouseClientBuilder.defaultExecutor;
    }

    /**
     * Submits task for execution. Depending on
     * {@link com.clickhouse.client.config.ClickHouseDefaults#ASYNC}, it may or may
     * not use {@link #getExecutorService()} to run the task in a separate thread.
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
     * @param format       output format to use
     * @param compression  compression algorithm to use
     * @param file         output file
     * @return non-null future object to get result
     * @throws IllegalArgumentException if any of server, tableOrQuery, and output
     *                                  is null
     * @throws CompletionException      when error occurred during execution
     * @throws IOException              when failed to create the file or its parent
     *                                  directories
     */
    static CompletableFuture<ClickHouseResponseSummary> dump(ClickHouseNode server, String tableOrQuery,
            ClickHouseFormat format, ClickHouseCompression compression, String file) throws IOException {
        return dump(server, tableOrQuery, format, compression, ClickHouseUtils.getFileOutputStream(file));
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
        final ClickHouseNode theServer = ClickHouseCluster.probe(server);

        final String theQuery = tableOrQuery.trim();

        return submit(() -> {
            try (ClickHouseClient client = newInstance(theServer.getProtocol())) {
                ClickHouseRequest<?> request = client.connect(theServer).compressServerResponse(
                        compression != null && compression != ClickHouseCompression.NONE, compression).format(format);
                // FIXME what if the table name is `try me`?
                if (theQuery.indexOf(' ') < 0) {
                    request.table(theQuery);
                } else {
                    request.query(theQuery);
                }

                try (ClickHouseResponse response = request.execute().get()) {
                    response.pipe(output, 8192);
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
                try {
                    output.close();
                } catch (Exception e) {
                    // ignore
                }
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
     * @throws FileNotFoundException    when file not found
     */
    static CompletableFuture<ClickHouseResponseSummary> load(ClickHouseNode server, String table,
            ClickHouseFormat format, ClickHouseCompression compression, String file) throws FileNotFoundException {
        return load(server, table, format, compression, ClickHouseUtils.getFileInputStream(file));
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
        final ClickHouseNode theServer = ClickHouseCluster.probe(server);

        return submit(() -> {
            InputStream input = null;
            // must run in async mode so that we won't hold everything in memory
            try (ClickHouseClient client = ClickHouseClient.builder()
                    .nodeSelector(ClickHouseNodeSelector.of(theServer.getProtocol()))
                    .option(ClickHouseClientOption.ASYNC, true).build()) {
                ClickHousePipedStream stream = ClickHouseDataStreamFactory.getInstance()
                        .createPipedStream(client.getConfig());
                // execute query in a separate thread(because async is explicitly set to true)
                CompletableFuture<ClickHouseResponse> future = client.connect(theServer).write().table(table)
                        .decompressClientRequest(compression != null && compression != ClickHouseCompression.NONE,
                                compression)
                        .format(format).data(input = stream.getInput()).execute();
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
        final ClickHouseNode theServer = ClickHouseCluster.probe(server);

        return submit(() -> {
            try (ClickHouseClient client = newInstance(theServer.getProtocol());
                    ClickHouseResponse response = client.connect(theServer).write().table(table)
                            .decompressClientRequest(compression != null && compression != ClickHouseCompression.NONE,
                                    compression)
                            .format(format).data(input).execute().get()) {
                return response.getSummary();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (CancellationException e) {
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (ExecutionException e) {
                throw ClickHouseException.of(e, theServer);
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
        final ClickHouseNode theServer = ClickHouseCluster.probe(server);

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
                    try (ClickHouseResponse resp = request.query(query).execute().get()) {
                        list.add(resp.getSummary());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (CancellationException e) {
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (ExecutionException e) {
                throw ClickHouseException.of(e, theServer);
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
        final ClickHouseNode theServer = ClickHouseCluster.probe(server);

        return submit(() -> {
            // set async to false so that we don't have to create additional thread
            try (ClickHouseClient client = ClickHouseClient.builder()
                    .nodeSelector(ClickHouseNodeSelector.of(theServer.getProtocol()))
                    .option(ClickHouseClientOption.ASYNC, false).build();
                    ClickHouseResponse resp = client.connect(theServer).format(ClickHouseFormat.RowBinary).query(sql)
                            .params(params).execute().get()) {
                return resp.getSummary();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (CancellationException e) {
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (ExecutionException e) {
                throw ClickHouseException.of(e, theServer);
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
        final ClickHouseNode theServer = ClickHouseCluster.probe(server);

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
                    try (ClickHouseResponse resp = request.params(arr).execute().get()) {
                        list.add(resp.getSummary());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (CancellationException e) {
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (ExecutionException e) {
                throw ClickHouseException.of(e, theServer);
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
        final ClickHouseNode theServer = ClickHouseCluster.probe(server);

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
                    try (ClickHouseResponse resp = request.query(builder.toString()).execute().get()) {
                        list.add(resp.getSummary());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (CancellationException e) {
                throw ClickHouseException.forCancellation(e, theServer);
            } catch (ExecutionException e) {
                throw ClickHouseException.of(e, theServer);
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
     * Connects to a ClickHouse server defined by the given
     * {@link java.util.function.Function}. You can pass either
     * {@link ClickHouseCluster} or {@link ClickHouseNode} here, as both of them
     * implemented the same interface.
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
        return new ClickHouseRequest<>(this, ClickHouseChecker.nonNull(nodeFunc, "nodeFunc"), false);
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
        if (server != null && timeout > 0) {
            server = ClickHouseCluster.probe(server, timeout);
            try (ClickHouseResponse resp = connect(server) // create request
                    .option(ClickHouseClientOption.ASYNC, false) // use current thread
                    .option(ClickHouseClientOption.CONNECTION_TIMEOUT, timeout)
                    .option(ClickHouseClientOption.SOCKET_TIMEOUT, timeout)
                    .option(ClickHouseClientOption.MAX_BUFFER_SIZE, 8) // actually 4 bytes should be enough
                    .option(ClickHouseClientOption.MAX_QUEUED_BUFFERS, 1) // enough with only one buffer
                    .format(ClickHouseFormat.TabSeparated)
                    .query("SELECT 1 FORMAT TabSeparated").execute()
                    .get(timeout, TimeUnit.MILLISECONDS)) {
                return true;
            } catch (Exception e) {
                // ignore
            }
        }

        return false;
    }

    @Override
    void close();
}
