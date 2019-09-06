package ru.yandex.clickhouse;

import ru.yandex.clickhouse.util.ClickHouseRowBinaryInputStream;

import java.sql.SQLException;
import java.util.*;

class Reader extends ConfigurableApi<Reader> {

    private List<ClickHouseExternalData> externalData = new ArrayList<ClickHouseExternalData>();

    Reader(ClickHouseStatementImpl statement) {
        super(statement);
    }

    public Reader withExternalData(List<ClickHouseExternalData> data) {
        this.externalData = new ArrayList<ClickHouseExternalData>();
        if (null != data) {
            this.externalData.addAll(data);
        }
        return this;
    }

    public Reader addExternalData(ClickHouseExternalData data) {
        externalData.add(data);
        return this;
    }

    /**
     * A shortcut method to execute query, and convert result into desired business-object
     */
    public <T> T executeQuery(String sql, ResponseFactory<T> responseFactory) throws SQLException {
        return null;
    }

    /**
     * Returns adopted stream for reading rows in RowBinary format
     * @param sql SQL query, without FORMAT RowBinary
     */
    public ClickHouseRowBinaryInputStream executeQuery(String sql) throws SQLException {
        return statement.executeQueryClickhouseRowBinaryStream(sql, getAdditionalDBParams(), getRequestParams());
    }

}
