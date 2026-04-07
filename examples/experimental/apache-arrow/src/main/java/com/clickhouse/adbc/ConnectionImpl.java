package com.clickhouse.adbc;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ConnectionImpl implements AdbcConnection  {
    @Override
    public AdbcStatement createStatement() throws AdbcException {
        return null;
    }

    @Override
    public ArrowReader getInfo(int @Nullable [] ints) throws AdbcException {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
