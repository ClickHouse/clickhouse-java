package com.clickhouse.adbc;

import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;

import java.util.Map;

public class ClickHouseAdbcDriver implements AdbcDriver {

    @Override
    public AdbcDatabase open(Map<String, Object> map) throws AdbcException {
        return null;
    }
}
