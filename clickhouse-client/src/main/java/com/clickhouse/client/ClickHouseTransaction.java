package com.clickhouse.client;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.value.UnsignedLong;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * This class represents a transaction in ClickHouse. Besides transaction ID
 * {@code Tuple(snapshotVersion UInt64, localTxCounter UInt64, hostId UUID)}, it
 * also contains session ID and references to the connected server and client
 * for issuing queries.
 */
@Deprecated
public final class ClickHouseTransaction implements Serializable {
    /**
     * This class encapsulates transaction ID, which is defined as
     * {@code Tuple(snapshotVersion UInt64, localTxCounter UInt64, hostId UUID)}.
     */
    public static class XID implements Serializable {
        private static final long serialVersionUID = 4907177669971332404L;

        public static final XID EMPTY = new XID(0L, 0L, new UUID(0L, 0L).toString());

        /**
         * Creates transaction ID from the given tuple.
         *
         * @param list non-null tuple with 3 elements
         * @return non-null transaction ID
         */
        public static XID of(List<?> list) {
            if (list == null || list.size() != 3) {
                throw new IllegalArgumentException(
                        "Non-null tuple with 3 elements(long, long, String) is required");
            }
            long snapshotVersion = ((UnsignedLong) list.get(0)).longValue();
            long localTxCounter = ((UnsignedLong) list.get(1)).longValue();
            String hostId = String.valueOf(list.get(2));
            if (EMPTY.snapshotVersion == snapshotVersion && EMPTY.localTxCounter == localTxCounter
                    && EMPTY.hostId.equals(hostId)) {
                return EMPTY;
            }
            return new XID(snapshotVersion, localTxCounter, hostId);
        }

        private final long snapshotVersion;
        private final long localTxCounter;
        private final String hostId;

        protected XID(long snapshotVersion, long localTxCounter, String hostId) {
            this.snapshotVersion = snapshotVersion;
            this.localTxCounter = localTxCounter;
            this.hostId = hostId;
        }

        public long getSnapshotVersion() {
            return snapshotVersion;
        }

        public long getLocalTransactionCounter() {
            return localTxCounter;
        }

        public String getHostId() {
            return hostId;
        }

        public String asTupleString() {
            return new StringBuilder().append('(').append(snapshotVersion).append(',').append(localTxCounter)
                    .append(",'").append(hostId).append("')").toString();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = prime + (int) (snapshotVersion ^ (snapshotVersion >>> 32));
            result = prime * result + (int) (localTxCounter ^ (localTxCounter >>> 32));
            result = prime * result + hostId.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            XID other = (XID) obj;
            return snapshotVersion == other.snapshotVersion && localTxCounter == other.localTxCounter
                    && hostId.equals(other.hostId);
        }

        @Override
        public String toString() {
            return new StringBuilder().append("TransactionId [snapshotVersion=").append(snapshotVersion)
                    .append(", localTxCounter=").append(localTxCounter).append(", hostId=").append(hostId).append("]@")
                    .append(hashCode()).toString();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClickHouseTransaction.class);
    private static final long serialVersionUID = -4618710299106666829L;

    private static final String[] NAMES = new String[] {
            "New", "Active", "Failed", "Commited", "RolledBack"
    };

    static final String QUERY_SELECT_TX_ID = "SELECT transactionID()";

    public static final String COMMAND_BEGIN = "BEGIN";
    public static final String COMMAND_COMMIT = "COMMIT";
    public static final String COMMAND_ROLLBACK = "ROLLBACK";

    // transaction state
    public static final int NEW = 0;
    public static final int ACTIVE = 1;
    public static final int FAILED = 2;
    public static final int COMMITTED = 3;
    public static final int ROLLED_BACK = 4;

    // reserved CSN - see
    // https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/TransactionID.h
    public static final long CSN_UNKNOWN = 0L; // For transactions that are probably not committed (yet)
    public static final long CSN_PREHISTORIC = 1L; // For changes were made without creating a transaction
    // Special reserved values
    public static final long CSN_COMMITTING = 2L;
    public static final long CSN_EVERYTHING_VISIBLE = 3L;
    public static final long CSN_MAX_RESERVED = 32L;

    public static final String SETTING_IMPLICIT_TRANSACTION = "implicit_transaction";
    public static final String SETTING_THROW_ON_UNSUPPORTED_QUERY_INSIDE_TRANSACTION = "throw_on_unsupported_query_inside_transaction";
    public static final String SETTING_WAIT_CHANGES_BECOME_VISIBLE_AFTER_COMMIT_MODE = "wait_changes_become_visible_after_commit_mode";

    /**
     * Updates the given request by enabling or disabling implicit transaction.
     *
     * @param request non-null request to update
     * @param enable  whether enable implicit transaction or not
     * @throws ClickHouseException when failed to enable or disable implicit
     *                             transaction
     */
    static void setImplicitTransaction(ClickHouseRequest<?> request, boolean enable) throws ClickHouseException {
        if (enable) {
            request.set(SETTING_IMPLICIT_TRANSACTION, 1).transaction(null);
        } else {
            request.removeSetting(SETTING_IMPLICIT_TRANSACTION);
        }
    }

    private final ClickHouseNode server;
    private final String sessionId;
    private final int timeout;
    private final boolean implicit;
    private final AtomicReference<XID> id;
    private final AtomicInteger state;

    /**
     * Constructs a unique transaction in {@link #ACTIVE} state.
     * {@link ClickHouseRequestManager#createSessionId()} will be used to ensure
     * uniquness of the transaction.
     *
     * @param server   non-null server of the transaction
     * @param timeout  transaction timeout
     * @param implicit whether it's an implicit transaction or not
     * @throws ClickHouseException when failed to start transaction
     */
    protected ClickHouseTransaction(ClickHouseNode server, int timeout, boolean implicit) throws ClickHouseException {
        this.server = server;
        this.sessionId = ClickHouseRequestManager.getInstance().createSessionId();
        this.timeout = timeout < 1 ? 0 : timeout;
        this.implicit = implicit;
        this.id = new AtomicReference<>(XID.EMPTY);
        this.state = new AtomicInteger(NEW);

        try {
            id.updateAndGet(x -> {
                boolean success = false;
                try {
                    issue("BEGIN TRANSACTION", false, Collections.emptyMap());
                    XID txId = XID.of(issue(QUERY_SELECT_TX_ID).getValue(0).asTuple());

                    if (XID.EMPTY.equals(txId)) {
                        throw new ClickHouseTransactionException(
                                ClickHouseTransactionException.ERROR_UNKNOWN_STATUS_OF_TRANSACTION,
                                ClickHouseUtils.format("Failed to start transaction(implicit=%s)", implicit), this);
                    }
                    success = state.compareAndSet(NEW, ACTIVE);
                    return txId;
                } catch (ClickHouseException e) {
                    throw new IllegalStateException(e);
                } finally {
                    if (!success) {
                        state.compareAndSet(NEW, FAILED);
                    }
                }
            });
            log.debug("Began transaction(implicit=%s): %s", this.implicit, this);
        } catch (IllegalStateException e) {
            if (e.getCause() instanceof ClickHouseException) {
                throw (ClickHouseException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Constructs a transaction in {@link #NEW} state, hence {@link #begin()} or
     * {@link #begin(Map)} must be called before commit/rollback and
     * {@link #isImplicit()} is always {@code false}.
     *
     * @param server    non-null server of the transaction
     * @param sessionId non-empty session ID for the transaction
     * @param timeout   transaction timeout
     * @param id        optional transaction ID
     */
    protected ClickHouseTransaction(ClickHouseNode server, String sessionId, int timeout, XID id) {
        this.server = server;
        this.sessionId = sessionId;
        this.timeout = timeout < 1 ? 0 : timeout;
        this.implicit = false;
        if (id == null || XID.EMPTY.equals(id)) {
            this.id = new AtomicReference<>(XID.EMPTY);
            this.state = new AtomicInteger(NEW);
        } else {
            this.id = new AtomicReference<>(id);
            this.state = new AtomicInteger(ACTIVE);
        }
    }

    /**
     * Ensures client and server are using the exact same transaction ID.
     *
     * @throws ClickHouseException when transaction ID is inconsistent between
     *                             client and server
     */
    protected void ensureTransactionId() throws ClickHouseException {
        if (!implicit) {
            XID serverTxId = XID.of(issue(QUERY_SELECT_TX_ID).getValue(0).asTuple());
            if (!serverTxId.equals(id.get())) {
                throw new ClickHouseTransactionException(
                        ClickHouseUtils.format(
                                "Inconsistent transaction ID - client expected %s but found %s on server.",
                                id.get(), serverTxId),
                        this);
            }
        }
    }

    /**
     * Issues transaction related query. Same as
     * {@code issue(command, true, Collections.emptyMap())}.
     *
     * @param command non-empty transaction related query
     * @return non-null record
     * @throws ClickHouseException when failed to issue the query
     */
    protected final ClickHouseRecord issue(String command) throws ClickHouseException {
        return issue(command, true, Collections.emptyMap());
    }

    /**
     * Issues transaction related query.
     *
     * @param command      non-empty transaction related query
     * @param sessionCheck whether to enable session check
     * @param settings     optional server settings
     * @return non-null record
     * @throws ClickHouseException when failed to issue the query
     */
    protected ClickHouseRecord issue(String command, boolean sessionCheck, Map<String, Serializable> settings)
            throws ClickHouseException {
        ClickHouseRecord result = ClickHouseRecord.EMPTY;
        try (ClickHouseResponse response = ClickHouseClient.newInstance(server.getProtocol()).read(server)
                .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                .settings(settings).session(sessionId, sessionCheck, timeout > 0 ? timeout : null)
                .query(command).executeAndWait()) {
            Iterator<ClickHouseRecord> records = response.records().iterator();
            if (records.hasNext()) {
                result = records.next();
            }
        } catch (ClickHouseException e) {
            switch (e.getErrorCode()) {
                case ClickHouseException.ERROR_SESSION_NOT_FOUND:
                    throw new ClickHouseTransactionException(
                            "Invalid transaction due to session not found or timed out", e.getCause(), this);
                case ClickHouseTransactionException.ERROR_INVALID_TRANSACTION:
                case ClickHouseTransactionException.ERROR_UNKNOWN_STATUS_OF_TRANSACTION:
                    throw new ClickHouseTransactionException(e.getErrorCode(), e.getMessage(), e.getCause(), this);
                default:
                    break;
            }
            throw e;
        }
        return result;
    }

    /**
     * Gets current transaction ID.
     *
     * @return non-null transaction ID
     */
    public XID getId() {
        return id.get();
    }

    /**
     * Gets server of the transaction.
     *
     * @return non-null server
     */
    public ClickHouseNode getServer() {
        return server;
    }

    /**
     * Gets session id of the transaction.
     *
     * @return non-empty session id
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Gets transaction state, one of {@link #NEW}, {@link #ACTIVE},
     * {@link #COMMITTED}, {@link #ROLLED_BACK}, or {@link #FAILED}.
     *
     * @return transaction state
     */
    public int getState() {
        return state.get();
    }

    /**
     * Gets transaction timeout in seconds.
     *
     * @return transaction timeout in seconds, zero or negative number means
     *         {@code default_session_timeout} as defined on server
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Checks if the transaction is implicit or not.
     *
     * @return true if it's an implicit transaction; false otherwise
     */
    public boolean isImplicit() {
        return implicit;
    }

    /**
     * Checks whether the transation's state is {@link #NEW}.
     *
     * @return true if the state is {@link #NEW}; false otherwise
     */
    public boolean isNew() {
        return state.get() == NEW;
    }

    /**
     * Checks whether the transation's state is {@link #ACTIVE}.
     *
     * @return true if the state is {@link #ACTIVE}; false otherwise
     */
    public boolean isActive() {
        return state.get() == ACTIVE;
    }

    /**
     * Checks whether the transation's state is {@link #COMMITTED}.
     *
     * @return true if the state is {@link #COMMITTED}; false otherwise
     */
    public boolean isCommitted() {
        return state.get() == COMMITTED;
    }

    /**
     * Checks whether the transation's state is {@link #ROLLED_BACK}.
     *
     * @return true if the state is {@link #ROLLED_BACK}; false otherwise
     */
    public boolean isRolledBack() {
        return state.get() == ROLLED_BACK;
    }

    /**
     * Checks whether the transation's state is {@link #FAILED}.
     *
     * @return true if the state is {@link #FAILED}; false otherwise
     */
    public boolean isFailed() {
        return state.get() == FAILED;
    }

    /**
     * Aborts the transaction.
     */
    public void abort() {
        log.debug("Abort %s", this);
        int currentState = state.get();
        if (currentState == NEW) {
            log.debug("Skip since it's a new transaction which hasn't started yet");
            return;
        }

        id.updateAndGet(x -> {
            try (ClickHouseResponse response = ClickHouseClient.newInstance(server.getProtocol()).read(server)
                    .query("KILL TRANSACTION WHERE tid=" + x.asTupleString()).executeAndWait()) {
                // ignore
            } catch (ClickHouseException e) {
                log.warn("Failed to abort transaction %s", x.asTupleString());
            } finally {
                state.compareAndSet(currentState, FAILED);
            }
            return x;
        });
        log.debug("Aborted transaction: %s", this);
    }

    /**
     * Starts a new transaction. Same as {@code begin(Collections.emptyMap())}.
     *
     * @throws ClickHouseException when failed to begin new transaction
     */
    public void begin() throws ClickHouseException {
        begin(Collections.emptyMap());
    }

    /**
     * Starts a new transaction with optional server settings. It's a no-op when
     * calling against an {@link #ACTIVE} transaction.
     *
     * @param settings optional server settings
     * @throws ClickHouseException when failed to begin new transaction
     */
    public void begin(Map<String, Serializable> settings) throws ClickHouseException {
        log.debug("Begin %s", this);
        int currentState = state.get();
        if (currentState == ACTIVE) {
            log.debug("Skip since the transaction has been started already");
            return;
        } else if (currentState == FAILED) {
            throw new ClickHouseTransactionException(
                    "Cannot restart a failed transaction - please roll back or create a new transaction",
                    this);
        }

        try {
            id.updateAndGet(x -> {
                boolean success = false;
                XID txId = null;
                try {
                    // reuse existing transaction if any
                    txId = XID.of(issue(QUERY_SELECT_TX_ID, false, Collections.emptyMap()).getValue(0).asTuple());
                    if (XID.EMPTY.equals(txId)) {
                        issue("BEGIN TRANSACTION", true, settings);
                        txId = XID.of(issue(QUERY_SELECT_TX_ID).getValue(0).asTuple());
                    }

                    if (XID.EMPTY.equals(txId)) {
                        throw new ClickHouseTransactionException(
                                ClickHouseTransactionException.ERROR_UNKNOWN_STATUS_OF_TRANSACTION,
                                "Failed to start new transaction", this);
                    }
                    success = state.compareAndSet(currentState, ACTIVE);
                    return txId;
                } catch (ClickHouseException e) {
                    throw new IllegalStateException(e);
                } finally {
                    if (txId != null && !success) {
                        state.compareAndSet(currentState, FAILED);
                    }
                }
            });
            log.debug("Began new transaction: %s", this);
        } catch (IllegalStateException e) {
            if (e.getCause() instanceof ClickHouseException) {
                throw (ClickHouseException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Commits the transaction. Same as {@code commit(Collections.emptyMap())}.
     *
     * @throws ClickHouseException when failed to commit the transaction
     */
    public void commit() throws ClickHouseException {
        commit(Collections.emptyMap());
    }

    /**
     * Commits the transaction with optional server settings. It's a no-op when
     * calling against a {@link #COMMITTED} transaction.
     *
     * @param settings optional server settings
     * @throws ClickHouseException when failed to commit the transaction
     */
    public void commit(Map<String, Serializable> settings) throws ClickHouseException {
        log.debug("Commit %s", this);
        int currentState = state.get();
        if (currentState == COMMITTED) {
            log.debug("Skip since the transaction has been committed already");
            return;
        } else if (currentState != ACTIVE) {
            throw new ClickHouseTransactionException(
                    ClickHouseUtils.format("Cannot commit inactive transaction(state=%s)", NAMES[currentState]), this);
        }

        try {
            id.updateAndGet(x -> {
                boolean success = false;
                try {
                    ensureTransactionId();
                    issue(COMMAND_COMMIT, true, settings);
                    success = state.compareAndSet(currentState, COMMITTED);
                    return x;
                } catch (ClickHouseException e) {
                    throw new IllegalStateException(e);
                } finally {
                    if (!success) {
                        state.compareAndSet(currentState, FAILED);
                    }
                }
            });
        } catch (IllegalStateException e) {
            if (e.getCause() instanceof ClickHouseException) {
                throw (ClickHouseException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Rolls back the transaction. Same as {@code rollback(Collections.emptyMap())}.
     *
     * @throws ClickHouseException when failed to roll back the transaction
     */
    public void rollback() throws ClickHouseException {
        rollback(Collections.emptyMap());
    }

    /**
     * Rolls back the transaction with optional server settings. It's a no-op when
     * calling against a {@link #NEW} or {@link #ROLLED_BACK} transaction.
     *
     * @param settings optional server settings
     * @throws ClickHouseException when failed to roll back the transaction
     */
    public void rollback(Map<String, Serializable> settings) throws ClickHouseException {
        log.debug("Roll back %s", this);
        int currentState = state.get();
        if (currentState == NEW) {
            log.debug("Skip since the transaction has not started yet");
            return;
        } else if (currentState == ROLLED_BACK) {
            log.debug("Skip since the transaction has been rolled back already");
            return;
        } else if (currentState != ACTIVE && currentState != FAILED) {
            throw new ClickHouseTransactionException(
                    ClickHouseUtils.format("Cannot roll back inactive transaction(state=%s)", NAMES[currentState]),
                    this);
        }

        try {
            id.updateAndGet(x -> {
                boolean success = false;
                try {
                    ensureTransactionId();
                    issue(COMMAND_ROLLBACK, true, settings);
                    success = state.compareAndSet(currentState, ROLLED_BACK);
                    return x;
                } catch (ClickHouseException e) {
                    throw new IllegalStateException(e);
                } finally {
                    if (!success) {
                        state.compareAndSet(currentState, FAILED);
                    }
                }
            });
        } catch (IllegalStateException e) {
            if (e.getCause() instanceof ClickHouseException) {
                throw (ClickHouseException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Sets transaction snapshot with optional server settings. Same as
     * {@code snapshot(snapshotVersion, Collections.emptyMap())}.
     *
     * @param snapshotVersion snapshot version
     * @throws ClickHouseException when failed to set transaction snapshot
     */
    public void snapshot(long snapshotVersion) throws ClickHouseException {
        snapshot(snapshotVersion, Collections.emptyMap());
    }

    /**
     * Sets transaction snapshot with optional server settings, only works for
     * {@link #ACTIVE} transaction. Use {@code snapshot(CSN_EVERYTHING_VISIBLE)} to
     * read uncommitted data.
     *
     * @param snapshotVersion snapshot version
     * @param settings        optional server settings
     * @throws ClickHouseException when failed to set transaction snapshot
     */
    public void snapshot(long snapshotVersion, Map<String, Serializable> settings) throws ClickHouseException {
        log.debug("Set snapshot %d for %s", snapshotVersion, this);
        int currentState = state.get();
        if (currentState != ACTIVE) {
            throw new ClickHouseTransactionException(
                    ClickHouseUtils.format("Cannot set snapshot version for inactive transaction(state=%s)",
                            NAMES[currentState]),
                    this);
        }

        try {
            id.updateAndGet(x -> {
                boolean success = false;
                try {
                    ensureTransactionId();
                    issue("SET TRANSACTION SNAPSHOT " + snapshotVersion, true, settings);
                    success = true;
                    return x;
                } catch (ClickHouseException e) {
                    throw new IllegalStateException(e);
                } finally {
                    if (!success) {
                        state.compareAndSet(currentState, FAILED);
                    }
                }
            });
        } catch (IllegalStateException e) {
            if (e.getCause() instanceof ClickHouseException) {
                throw (ClickHouseException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + server.getBaseUri().hashCode();
        result = prime * result + sessionId.hashCode();
        result = prime * result + timeout;
        result = prime * result + id.get().hashCode();
        result = prime * result + state.get();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseTransaction other = (ClickHouseTransaction) obj;
        return server.isSameEndpoint(other.server) && sessionId.equals(other.sessionId)
                && timeout == other.timeout && id.get().equals(other.id.get()) && state.get() == other.state.get();
    }

    @Override
    public String toString() {
        return new StringBuilder().append("ClickHouseTransaction [id=").append(id.get().asTupleString())
                .append(", session=").append(sessionId).append(", timeout=").append(timeout).append(", state=")
                .append(NAMES[state.get()]).append(", server=").append(server.getBaseUri()).append("]@")
                .append(hashCode()).toString();
    }
}
