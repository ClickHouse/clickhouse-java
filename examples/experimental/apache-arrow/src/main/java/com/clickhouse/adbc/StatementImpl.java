package com.clickhouse.adbc;

import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;

public class StatementImpl implements AdbcStatement {
    @Override
    public QueryResult executeQuery() throws AdbcException {
        return null;
    }

    @Override
    public UpdateResult executeUpdate() throws AdbcException {
        return null;
    }

    @Override
    public void prepare() throws AdbcException {

    }

    @Override
    public void close() throws Exception {

    }
}
