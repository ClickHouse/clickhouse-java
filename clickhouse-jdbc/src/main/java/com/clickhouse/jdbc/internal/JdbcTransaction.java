package com.clickhouse.jdbc.internal;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseRequestManager;
import com.clickhouse.client.ClickHouseTransaction;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.jdbc.SqlExceptionUtils;

@Deprecated
public class JdbcTransaction {
    static final String ACTION_COMMITTED = "committed";
    static final String ACTION_ROLLBACK = "rolled back";

    static final String ERROR_TX_NOT_STARTED = "Transaction not started";
    static final String ERROR_TX_STARTED = "Transaction has been started";

    protected final ClickHouseTransaction tx;
    protected final String id;
    protected final List<String> queries;
    protected final List<JdbcSavepoint> savepoints;

    JdbcTransaction() {
        this(null);
    }

    public JdbcTransaction(ClickHouseTransaction tx) {
        this.tx = tx;
        this.id = tx != null ? tx.getId().asTupleString() : ClickHouseRequestManager.getInstance().createUniqueId();
        this.queries = new LinkedList<>();
        this.savepoints = new LinkedList<>();
    }

    public boolean isNew() {
        return this.queries.isEmpty() && this.savepoints.isEmpty()
                && (this.tx == null || this.tx.isNew() || this.tx.isActive());
    }

    public void commit(Logger log) throws SQLException {
        if (this.tx != null) {
            try {
                this.tx.commit();
            } catch (ClickHouseException e) {
                throw SqlExceptionUtils.handle(e);
            }
        } else {
            logTransactionDetails(log, ACTION_COMMITTED);
        }
        clear();
    }

    public void rollback(Logger log) throws SQLException {
        if (this.tx != null) {
            try {
                this.tx.rollback();
            } catch (ClickHouseException e) {
                throw SqlExceptionUtils.handle(e);
            }
        } else {
            logTransactionDetails(log, JdbcTransaction.ACTION_ROLLBACK);
        }
        clear();
    }

    synchronized List<String> getQueries() {
        return Collections.unmodifiableList(queries);
    }

    synchronized List<JdbcSavepoint> getSavepoints() {
        return Collections.unmodifiableList(savepoints);
    }

    synchronized void logSavepointDetails(Logger log, JdbcSavepoint s, String action) {
        log.warn(
                "[JDBC Compliant Mode] Savepoint(id=%d, name=%s) of transaction [%s](%d queries & %d savepoints) is %s.",
                s.id, s.name, id, queries.size(), savepoints.size(), action);
    }

    synchronized void logTransactionDetails(Logger log, String action) {
        if (tx != null) {
            log.debug("%s (%d queries & %d savepoints) is %s", tx, queries.size(),
                    savepoints.size(), action);
        } else {
            log.warn("[JDBC Compliant Mode] Transaction [%s] (%d queries & %d savepoints) is %s.", id, queries.size(),
                    savepoints.size(), action);
        }

        if (log.isDebugEnabled()) {
            log.debug(() -> {
                log.debug("[JDBC Compliant Mode] Transaction [%s] is %s - begin", id, action);
                int total = queries.size();
                int counter = 1;
                for (String queryId : queries) {
                    log.debug("    '%s', -- query (%d of %d) in transaction [%s]", queryId, counter++, total, id);
                }

                total = savepoints.size();
                counter = 1;
                for (JdbcSavepoint savepoint : savepoints) {
                    log.debug("    %s (%d of %d) in transaction [%s]", savepoint, counter++, total, id);
                }
                return ClickHouseUtils.format("[JDBC Compliant Mode] Transaction [%s] is %s - end", id, action);
            });
        }
    }

    synchronized String newQuery(String queryId) {
        if (ClickHouseChecker.isNullOrEmpty(queryId) || queries.contains(queryId)) {
            queryId = ClickHouseRequestManager.getInstance().createQueryId();
        }

        queries.add(queryId);

        return queryId;
    }

    synchronized JdbcSavepoint newSavepoint(String name) {
        JdbcSavepoint savepoint = new JdbcSavepoint(queries.size(), name);
        this.savepoints.add(savepoint);
        return savepoint;
    }

    synchronized void toSavepoint(JdbcSavepoint savepoint) throws SQLException {
        if (tx != null) {
            try {
                tx.rollback();
            } catch (ClickHouseException e) {
                throw SqlExceptionUtils.handle(e);
            }
        }
        boolean found = false;
        Iterator<JdbcSavepoint> it = savepoints.iterator();
        while (it.hasNext()) {
            JdbcSavepoint s = it.next();
            if (found) {
                it.remove();
            } else if (s == savepoint) {
                found = true;
                it.remove();
            }
        }

        if (!found) {
            throw SqlExceptionUtils.clientError("Invalid savepoint: " + savepoint);
        }
        queries.subList(savepoint.id, queries.size()).clear();
    }

    synchronized void clear() {
        this.queries.clear();
        this.savepoints.clear();
    }
}
