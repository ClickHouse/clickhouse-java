package ru.yandex.clickhouse;

import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.util.*;

@SuppressWarnings("unchecked")
class ConfigurableApi<T> {

    protected final ClickHouseStatementImpl statement;
    private Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<ClickHouseQueryParam, String>();
    private List<ClickHouseExternalData> externalData = new ArrayList<ClickHouseExternalData>();

    ConfigurableApi(ClickHouseStatementImpl statement) {
        this.statement = statement;
    }

    public T withDbParams(Map<ClickHouseQueryParam, String> dbParams) {
        this.additionalDBParams = null == dbParams ? new HashMap<ClickHouseQueryParam, String>() : dbParams;
        return (T) this;
    }

    public T withExternalData(List<ClickHouseExternalData> data) {
        this.externalData = null == data ? new ArrayList<ClickHouseExternalData>() : data;
        return (T) this;
    }

    public T addDbParam(ClickHouseQueryParam param, String value) {
        additionalDBParams.put(param, value);
        return (T) this;
    }

    public T addExternalData(ClickHouseExternalData data) {
        externalData.add(data);
        return (T) this;
    }
}
