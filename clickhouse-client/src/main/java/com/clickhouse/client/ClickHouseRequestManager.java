package com.clickhouse.client;

import java.util.UUID;

import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;

/**
 * Request manager is responsible for generating query and session ID, as well
 * as transaction creation. {@link java.util.ServiceLoader} will search and
 * instantiate customized request manager first, and then fall back to default
 * implementation if no luck.
 */
@Deprecated
public class ClickHouseRequestManager {
    /**
     * Inner class for static initialization.
     */
    static final class InstanceHolder {
        private static final ClickHouseRequestManager instance = ClickHouseUtils
                .getService(ClickHouseRequestManager.class, ClickHouseRequestManager::new);

        private InstanceHolder() {
        }
    }

    /**
     * Gets instance of request manager.
     *
     * @return non-null request manager
     */
    public static ClickHouseRequestManager getInstance() {
        return InstanceHolder.instance;
    }

    /**
     * Creates a new query ID. By default, it simply forwards the call to
     * {@link #createUniqueId()}.
     *
     * @return non-null unique query ID
     */
    public String createQueryId() {
        return createUniqueId();
    }

    /**
     * Creates a new session ID. By default, it simply forwards the call to
     * {@link #createUniqueId()}.
     *
     * @return non-null unique session ID
     */
    public String createSessionId() {
        return createUniqueId();
    }

    /**
     * Creates a global, URL-safe unique ID. By default it uses
     * {@link UUID#randomUUID()} for least dependency, but you can override this
     * method to use any other alternatives like TimeUUID, snowflake and maybe
     * NanoID.
     *
     * @return non-empty global unique ID
     */
    public String createUniqueId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Creates an implicit transaction.
     *
     * @param request non-null request
     * @return non-null new transaction
     * @throws ClickHouseException when failed to create implicit transaction
     */
    public ClickHouseTransaction createImplicitTransaction(ClickHouseRequest<?> request) throws ClickHouseException {
        return new ClickHouseTransaction(ClickHouseChecker.nonNull(request, "Request").getServer(),
                request.getConfig().getTransactionTimeout(), true);
    }

    /**
     * Creates a new transaction. Same as {@code createTransaction(request, 0)}.
     *
     * @param request non-null request
     * @return non-null new transaction
     * @throws ClickHouseException when failed to create transaction
     */
    public ClickHouseTransaction createTransaction(ClickHouseRequest<?> request) throws ClickHouseException {
        return createTransaction(request, 0);
    }

    /**
     * Creates a new transaction. Unlike
     * {@link #getOrStartTransaction(ClickHouseRequest, int)}, the transaction's
     * state is {@link ClickHouseTransaction#NEW} and it's not bounded with the
     * request.
     *
     * @param request non-null request
     * @param timeout transaction timeout in seconds, zero or negative number
     *                means {@code request.getConfig().getTransactionTimeout()}
     * @return non-null new transaction
     * @throws ClickHouseException when failed to create transaction
     */
    public ClickHouseTransaction createTransaction(ClickHouseRequest<?> request, int timeout)
            throws ClickHouseException {
        return createTransaction(ClickHouseChecker.nonNull(request, "Request").getServer(),
                request.getConfig().getTransactionTimeout());
    }

    /**
     * Creates a new transaction. {@link #createSessionId()} will be called
     * to start a new session just for the transaction.
     *
     * @param server  non-null server
     * @param timeout transaction timeout in seconds
     * @return non-null new transaction
     * @throws ClickHouseException when failed to create transaction
     */
    public ClickHouseTransaction createTransaction(ClickHouseNode server, int timeout) throws ClickHouseException {
        return new ClickHouseTransaction(ClickHouseChecker.nonNull(server, "Server"), createSessionId(),
                timeout > 0 ? timeout : server.config.getTransactionTimeout(), null);
    }

    /**
     * Gets or starts a new transaction. Same as
     * {@code getOrStartTransaction(request, 0)}.
     *
     * @param request non-null request
     * @return non-null transaction
     * @throws ClickHouseException when failed to get or start transaction
     */
    public ClickHouseTransaction getOrStartTransaction(ClickHouseRequest<?> request) throws ClickHouseException {
        return getOrStartTransaction(request, 0);
    }

    /**
     * Gets or starts a new transaction. {@link #createSessionId()} will be called
     * to when a new transaction is created.
     *
     * @param request non-null request
     * @param timeout transaction timeout in seconds, zero or negative number
     *                means {@code request.getConfig().getTransactionTimeout()}
     * @return non-null transaction in {@link ClickHouseTransaction#ACTIVE} state
     * @throws ClickHouseException when failed to get or start transaction
     */
    public ClickHouseTransaction getOrStartTransaction(ClickHouseRequest<?> request, int timeout)
            throws ClickHouseException {
        if (timeout < 1) {
            timeout = request.getConfig().getTransactionTimeout();
        }
        ClickHouseTransaction tx = ClickHouseChecker.nonNull(request, "Request").getTransaction();
        if (tx != null && tx.getTimeout() == timeout) {
            return tx;
        }

        tx = createTransaction(request.getServer(), timeout);
        tx.begin();
        request.transaction(tx);
        return tx;
    }
}
