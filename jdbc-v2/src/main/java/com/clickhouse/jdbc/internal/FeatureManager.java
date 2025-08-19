package com.clickhouse.jdbc.internal;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class FeatureManager {

    private final JdbcConfiguration configuration;

    public FeatureManager(JdbcConfiguration configuration) {
        this.configuration = configuration;
    }

    public void unsupportedFeatureThrow(String featureName) throws SQLException {
        unsupportedFeatureThrow(featureName, true);
    }

    public void unsupportedFeatureThrow(String methodName, boolean doCheck) throws SQLException {
        if (doCheck && !configuration.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException(methodName + " is not supported.",
                    ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }
}