package ru.yandex.clickhouse;

import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.util.Collections;
import java.util.List;
import java.util.Map;

class ConfigurableApi<T> {

    protected final ClickHouseStatementImpl statement;
    private Map<ClickHouseQueryParam, String> additionalDBParams;
    private List<ClickHouseExternalData> externalData;

    ConfigurableApi(ClickHouseStatementImpl statement) {
        this.statement = statement;
    }

    public T withDbParams(Map<ClickHouseQueryParam, String> dbParams) {
        this.additionalDBParams = null == dbParams ? Collections.<ClickHouseQueryParam, String>emptyMap() : dbParams;
        return (T) this;
    }

    public T withExternalData(List<ClickHouseExternalData> data) {
        this.externalData = null == data ? Collections.<ClickHouseExternalData>emptyList() : data;
        return (T) this;
    }
}
