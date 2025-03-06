package com.clickhouse.client;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseHealthCheckMethod;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * Base class for implementing a thread-safe ClickHouse client. It uses
 * {@link ReadWriteLock} to manage access to underlying connection.
 */
@Deprecated
public abstract class AbstractClient<T> implements ClickHouseClient {
    private static final Logger log = LoggerFactory.getLogger(AbstractClient.class);

    private boolean initialized = false;

    private ExecutorService executor = null;
    private ClickHouseConfig config = null;
    private ClickHouseNode server = null;
    private T connection = null;

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    private boolean measureRequestTime = false;

    private void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Please initialize the client first");
        }
    }

    // just for testing purpose
    final boolean isInitialized() {
        return initialized;
    }

    protected abstract boolean checkHealth(ClickHouseNode server, int timeout);

    protected CompletableFuture<ClickHouseResponse> failedResponse(Throwable ex) {
        CompletableFuture<ClickHouseResponse> future = new CompletableFuture<>();
        future.completeExceptionally(ex);
        return future;
    }

    /**
     * Gets executor service for this client.
     *
     * @return executor service
     * @throws IllegalStateException when the client is either closed or not
     *                               initialized
     */
    protected final ExecutorService getExecutor() {
        lock.readLock().lock();
        try {
            ensureInitialized();
            return executor;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets list of supported protocols.
     *
     * @return non-null list of supported protocols
     */
    protected abstract Collection<ClickHouseProtocol> getSupportedProtocols();

    /**
     * Gets current server.
     *
     * @return current server
     * @throws IllegalStateException when the client is either closed or not
     *                               initialized
     */
    protected final ClickHouseNode getServer() {
        lock.readLock().lock();
        try {
            ensureInitialized();
            return server;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if the underlying connection can be reused. In general, new connection
     * will be created when {@code connection} is {@code null} or
     * {@code requestServer} is different from {@code currentServer} - the existing
     * connection will be closed in the later case.
     *
     * @param connection    existing connection which may or may not be null
     * @param requestServer non-null requested server, returned from previous call
     *                      of {@code request.getServer()}
     * @param currentServer current server, same as {@code getServer()}
     * @param request       non-null request
     * @return true if the connection should NOT be changed(e.g. requestServer is
     *         same as currentServer); false otherwise
     */
    protected boolean checkConnection(T connection, ClickHouseNode requestServer, ClickHouseNode currentServer,
            ClickHouseRequest<?> request) {
        return connection != null && requestServer.equals(currentServer);
    }

    /**
     * Creates a new connection and optionally close existing connection. This
     * method will be called from {@link #getConnection(ClickHouseRequest)} as
     * needed.
     *
     * @param connection existing connection which may or may not be null
     * @param server     non-null requested server, returned from previous call of
     *                   {@code request.getServer()}
     * @param request    non-null request
     * @return new connection
     * @throws CompletionException when error occured
     */
    protected abstract T newConnection(T connection, ClickHouseNode server, ClickHouseRequest<?> request);

    /**
     * Closes a connection. This method will be called from {@link #close()}.
     *
     * @param connection connection to close
     * @param force      whether force to close the connection or not
     */
    protected abstract void closeConnection(T connection, boolean force);

    /**
     * Gets arguments required for async execution. This method will be executed in
     * current thread right before {@link #sendAsync(ClickHouseRequest, Object...)}.
     *
     * @param sealedRequest non-null sealed request
     * @return arguments required for async execution
     */
    protected Object[] getAsyncExecArguments(ClickHouseRequest<?> sealedRequest) {
        return new Object[0];
    }

    /**
     * Sends the request to server in a separate thread.
     *
     * @param sealedRequest non-null sealed request
     * @param args          arguments required for sending out the request
     * @return non-null response
     * @throws ClickHouseException when error server failed to process the request
     * @throws IOException         when error occurred sending the request
     */
    protected ClickHouseResponse sendAsync(ClickHouseRequest<?> sealedRequest, Object... args)
            throws ClickHouseException, IOException {
        return send(sealedRequest);
    }

    /**
     * Sends the request to server in a current thread.
     *
     * @param sealedRequest non-null sealed request
     * @return non-null response
     * @throws ClickHouseException when error server failed to process the request
     * @throws IOException         when error occurred sending the request
     */
    protected abstract ClickHouseResponse send(ClickHouseRequest<?> sealedRequest)
            throws ClickHouseException, IOException;

    /**
     * Gets a connection according to the given request.
     *
     * @param request non-null request
     * @return non-null connection
     * @throws CompletionException when error occured
     */
    protected final T getConnection(ClickHouseRequest<?> request) {
        ClickHouseNode newNode = ClickHouseChecker.nonNull(request, "request").getServer();
        lock.readLock().lock();
        try {
            ensureInitialized();
            if (checkConnection(connection, newNode, server, request)) {
                return connection;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            server = newNode;
            log.debug("Connecting to: %s", newNode);
            connection = newConnection(connection, server, request);
            log.debug("Connection established: %s", connection);

            return connection;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean accept(ClickHouseProtocol protocol) {
        for (ClickHouseProtocol p : getSupportedProtocols()) {
            if (p == protocol) {
                return true;
            }
        }
        return ClickHouseClient.super.accept(protocol);
    }

    @Override
    public ClickHouseRequest<?> read(Function<ClickHouseNodeSelector, ClickHouseNode> nodeFunc,
            Map<ClickHouseOption, Serializable> options) {
        lock.readLock().lock();
        try {
            ensureInitialized();
            return ClickHouseClient.super.read(nodeFunc, options);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public final ClickHouseConfig getConfig() {
        lock.readLock().lock();
        try {
            ensureInitialized();
            return config;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void init(ClickHouseConfig config) {
        log.debug("Initializing new client: %d", this.hashCode());
        ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME);
        measureRequestTime = config.getBoolOption(ClickHouseClientOption.MEASURE_REQUEST_TIME);
        lock.writeLock().lock();
        try {
            Collection<ClickHouseProtocol> protocols = getSupportedProtocols();
            this.config = new ClickHouseConfig(config.getAllOptions(), config.getDefaultCredentials(),
                    config.getNodeSelector() != ClickHouseNodeSelector.EMPTY || protocols.isEmpty()
                            ? config.getNodeSelector()
                            : ClickHouseNodeSelector.of(protocols, null),
                    config.getMetricRegistry().orElse(null));
            if (this.executor == null) { // only initialize once
                int threads = config.getMaxThreadsPerClient();
                long threadTTL = config.getLongOption(ClickHouseClientOption.MAX_CORE_THREAD_TTL);
                this.executor = threads < 1 ? ClickHouseClient.getExecutorService()
                        : ClickHouseUtils.newThreadPool(this, threads, threads, config.getMaxQueuedRequests(), threadTTL, true);
            }

            initialized = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public CompletableFuture<ClickHouseResponse> execute(ClickHouseRequest<?> request) {
        final long start = System.nanoTime();
        // sealedRequest is an immutable copy of the original request
        final ClickHouseRequest<?> sealedRequest = request.seal();

        if (sealedRequest.getConfig().isAsync()) {
            final Object[] args = getAsyncExecArguments(sealedRequest);
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return sendAsync(sealedRequest, args);
                } catch (ClickHouseException | IOException e) {
                    throw new CompletionException(ClickHouseException.of(e, sealedRequest.getServer()));
                } finally {
                    if (measureRequestTime) {
                        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                        if (elapsed > 1000) {
                            log.info("Request took long to execute: %s", elapsed);
                        }
                    }
                }
            }, getExecutor());
        } else {
            try {
                return CompletableFuture.completedFuture(send(sealedRequest));
            } catch (ClickHouseException | IOException e) {
                return failedResponse(ClickHouseException.of(e, sealedRequest.getServer()));
            } finally {
                if (measureRequestTime) {
                    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                    if (elapsed > 1000) {
                        log.info("Request took long to execute: %s", elapsed);
                    }
                }
            }
        }
    }

    @Override
    public final void close() {
        lock.readLock().lock();
        try {
            if (!initialized) {
                return;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            server = null;

            if (connection != null) {
                closeConnection(connection, false);
                connection = null;
            }

            // avoid shutting down shared thread pool
            if (executor != null && config.getMaxThreadsPerClient() > 0 && !executor.isTerminated()) {
                executor.shutdown();
            }
            executor = null;
        } catch (Exception e) {
            log.warn("Exception occurred when closing client", e);
        } finally {
            initialized = false;
            try {
                if (connection != null) {
                    closeConnection(connection, true);
                }

                if (executor != null && config.getMaxThreadsPerClient() > 0) {
                    executor.shutdownNow();
                }
            } finally {
                executor = null;
                connection = null;
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public boolean ping(ClickHouseNode server, int timeout) {
        if (server == null) {
            return false;
        } else if (server.config.getOption(ClickHouseClientOption.HEALTH_CHECK_METHOD,
                getConfig()) != ClickHouseHealthCheckMethod.PING) {
            return ClickHouseClient.super.ping(server, timeout);
        }

        if (server.getProtocol() == ClickHouseProtocol.ANY) {
            server = ClickHouseNode.probe(server.getHost(), server.getPort(), timeout);
        }
        return checkHealth(server, timeout);
    }
}
