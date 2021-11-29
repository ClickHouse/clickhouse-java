package com.clickhouse.jdbc.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.jdbc.SqlExceptionUtils;

public final class FakeTransaction {
    static final String ACTION_COMMITTED = "committed";
    static final String ACTION_ROLLBACK = "rolled back";

    static final String ERROR_TX_NOT_STARTED = "Transaction not started";

    static final int DEFAULT_TX_ISOLATION_LEVEL = Connection.TRANSACTION_READ_UNCOMMITTED;

    static final class FakeSavepoint implements Savepoint {
        final int id;
        final String name;

        FakeSavepoint(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public int getSavepointId() throws SQLException {
            if (name != null) {
                throw SqlExceptionUtils
                        .clientError("Cannot get ID from a named savepoint, use getSavepointName instead");
            }

            return id;
        }

        @Override
        public String getSavepointName() throws SQLException {
            if (name == null) {
                throw SqlExceptionUtils
                        .clientError("Cannot get name from an un-named savepoint, use getSavepointId instead");
            }

            return name;
        }

        @Override
        public String toString() {
            return new StringBuilder().append("FakeSavepoint [id=").append(id).append(", name=").append(name)
                    .append(']').toString();
        }
    }

    final String id;

    private final List<String> queries;
    private final List<FakeSavepoint> savepoints;

    FakeTransaction() {
        this.id = UUID.randomUUID().toString();
        this.queries = new LinkedList<>();
        this.savepoints = new ArrayList<>();
    }

    synchronized List<String> getQueries() {
        return new ArrayList<>(queries);
    }

    synchronized List<FakeSavepoint> getSavepoints() {
        return new ArrayList<>(savepoints);
    }

    synchronized void logSavepointDetails(Logger log, FakeSavepoint s, String action) {
        log.warn(
                "[JDBC Compliant Mode] Savepoint(id=%d, name=%s) of transaction [%s](%d queries & %d savepoints) is %s.",
                s.id, s.name, id, queries.size(), savepoints.size(), action);
    }

    synchronized void logTransactionDetails(Logger log, String action) {
        log.warn("[JDBC Compliant Mode] Transaction [%s](%d queries & %d savepoints) is %s.", id, queries.size(),
                savepoints.size(), action);

        log.debug(() -> {
            log.debug("[JDBC Compliant Mode] Transaction [%s] is %s - begin", id, action);
            int total = queries.size();
            int counter = 1;
            for (String queryId : queries) {
                log.debug("    '%s', -- query (%d of %d) in transaction [%s]", queryId, counter++, total, id);
            }

            total = savepoints.size();
            counter = 1;
            for (FakeSavepoint savepoint : savepoints) {
                log.debug("    %s (%d of %d) in transaction [%s]", savepoint, counter++, total, id);
            }
            return ClickHouseUtils.format("[JDBC Compliant Mode] Transaction [%s] is %s - end", id, action);
        });
    }

    synchronized String newQuery(String queryId) {
        if (queryId == null || queries.contains(queryId)) {
            queryId = UUID.randomUUID().toString();
        }

        queries.add(queryId);

        return queryId;
    }

    synchronized FakeSavepoint newSavepoint(String name) {
        FakeSavepoint savepoint = new FakeSavepoint(queries.size(), name);
        this.savepoints.add(savepoint);
        return savepoint;
    }

    synchronized void toSavepoint(FakeSavepoint savepoint) throws SQLException {
        boolean found = false;
        Iterator<FakeSavepoint> it = savepoints.iterator();
        while (it.hasNext()) {
            FakeSavepoint s = it.next();
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
