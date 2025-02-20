package com.clickhouse.jdbc.internal;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.jdbc.JdbcTypeMapping;
import com.clickhouse.jdbc.SqlExceptionUtils;
import com.clickhouse.jdbc.JdbcWrapper;

@Deprecated
public class ClickHouseParameterMetaData extends JdbcWrapper implements ParameterMetaData {
    protected final List<ClickHouseColumn> params;
    protected final JdbcTypeMapping mapper;
    protected final Map<String, Class<?>> typeMap;

    protected ClickHouseParameterMetaData(List<ClickHouseColumn> params, JdbcTypeMapping mapper,
            Map<String, Class<?>> typeMap) {
        this.params = ClickHouseChecker.nonNull(params, "Parameters");

        this.mapper = mapper;
        this.typeMap = typeMap;
    }

    protected ClickHouseColumn getParameter(int param) throws SQLException {
        if (param < 1 || param > params.size()) {
            throw SqlExceptionUtils.clientError(ClickHouseUtils
                    .format("Parameter index should between 1 and %d but we got %d", params.size(), param));
        }

        return params.get(param - 1);
    }

    @Override
    public int getParameterCount() throws SQLException {
        return params.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        ClickHouseColumn p = getParameter(param);
        if (p == null) {
            return ParameterMetaData.parameterNullableUnknown;
        }

        return p.isNullable() ? ParameterMetaData.parameterNullable : ParameterMetaData.parameterNoNulls;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        ClickHouseColumn p = getParameter(param);
        return p != null && p.getDataType().isSigned();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        ClickHouseColumn p = getParameter(param);
        return p != null ? p.getPrecision() : 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        ClickHouseColumn p = getParameter(param);
        return p != null ? p.getScale() : 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        ClickHouseColumn p = getParameter(param);
        return p != null ? mapper.toSqlType(p, typeMap) : Types.OTHER;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        ClickHouseColumn p = getParameter(param);
        return p != null ? mapper.toNativeType(p) : "<unknown>";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        ClickHouseColumn p = getParameter(param);
        return (p != null ? mapper.toJavaClass(p, typeMap) : Object.class).getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }
}
