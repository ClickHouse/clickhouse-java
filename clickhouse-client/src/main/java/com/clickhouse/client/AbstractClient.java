package com.clickhouse.client;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

/**
 * Base class for implementing a thread-safe ClickHouse client. It uses
 * {@link ReadWriteLock} to manage access to underlying connection.
 */
public abstract class AbstractClient<T> implements ClickHouseClient {
    private static final Logger log = LoggerFactory.getLogger(AbstractClient.class);

    private boolean initialized;

    private ExecutorService executor;
    private ClickHouseConfig config;
    private ClickHouseNode server;
    private T connection;

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    private void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Please initialize the client first");
        }
    }

    // just for testing purpose
    final boolean isInitialized() {
        return initialized;
    }

    /**
     * Gets executor service for this client.
     *
     * @return executor service
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
     * Gets current server.
     *
     * @return current server, could be null
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
     * Creates a new connection. This method will be called from
     * {@link #getConnection(ClickHouseRequest)} as needed.
     *
     * @param config non-null configuration
     * @param server non-null server
     * @return new connection
     * @throws CompletionException when error occured
     */
    protected abstract T newConnection(ClickHouseConfig config, ClickHouseNode server);

    /**
     * Closes a connection. This method will be called from {@link #close()}.
     *
     * @param connection connection to close
     * @param force      whether force to close the connection or not
     */
    protected abstract void closeConnection(T connection, boolean force);

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
            if (connection != null && newNode.equals(server)) {
                return connection;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            if (connection != null) {
                log.debug("Closing connection: %s", connection);
                closeConnection(connection, false);
            }

            server = newNode;
            log.debug("Connecting to: %s", newNode);
            connection = newConnection(config, server);
            log.debug("Connection established: %s", connection);

            return connection;
        } finally {
            lock.writeLock().unlock();
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
        ClickHouseChecker.nonNull(config, "config");

        lock.writeLock().lock();
        try {
            this.config = config;
            if (this.executor == null) { // only initialize once
                int threads = config.getMaxThreadsPerClient();
                this.executor = threads <= 0 ? ClickHouseClient.getExecutorService()
                        : ClickHouseUtils.newThreadPool(getClass().getSimpleName(), threads,
                                config.getMaxQueuedRequests());
            }

            initialized = true;
        } finally {
            lock.writeLock().unlock();
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

            if (executor != null) {
                executor.shutdown();
            }

            if (connection != null) {
                closeConnection(connection, false);
            }

            // shutdown* won't shutdown commonPool, so awaitTermination will always time out
            // on the other hand, for a client-specific thread pool, we'd better shut it
            // down for real
            if (executor != null && config.getMaxThreadsPerClient() > 0
                    && !executor.awaitTermination(config.getConnectionTimeout(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }

            executor = null;
            connection = null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.warn("Exception occurred when closing client", e);
        } finally {
            initialized = false;
            try {
                if (connection != null) {
                    closeConnection(connection, true);
                }

                if (executor != null) {
                    executor.shutdownNow();
                }
            } finally {
                executor = null;
                connection = null;
                lock.writeLock().unlock();
            }
        }
    }
}
