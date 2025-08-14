package com.clickhouse.jdbc.internal;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class FeatureManager {

    private final JdbcConfiguration configuration;

    public FeatureManager(JdbcConfiguration configuration) {
        this.configuration = configuration;
    }

    public void unsupportedFeatureThrow(String methodName) throws SQLException {
        if (!configuration.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException(methodName + " is not supported.",
                    ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }
}
