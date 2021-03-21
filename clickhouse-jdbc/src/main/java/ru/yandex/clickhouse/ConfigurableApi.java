package ru.yandex.clickhouse;

import java.util.HashMap;
import java.util.Map;

import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

abstract class ConfigurableApi<T> {

    protected final ClickHouseStatementImpl statement;
    private Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<>();
    private Map<String, String> additionalRequestParams = new HashMap<>();

    ConfigurableApi(ClickHouseStatementImpl statement) {
        this.statement = statement;
    }

    Map<String, String> getRequestParams() {
        return additionalRequestParams;
    }

    Map<ClickHouseQueryParam, String> getAdditionalDBParams() {
        return additionalDBParams;
    }

    public T addDbParam(ClickHouseQueryParam param, String value) {
        additionalDBParams.put(param, value);
        return getThis();
    }

    public T removeDbParam(ClickHouseQueryParam param) {
        additionalDBParams.remove(param);
        return getThis();
    }

    public T withDbParams(Map<ClickHouseQueryParam, String> dbParams) {
        this.additionalDBParams = new HashMap<>();
        if (null != dbParams) {
            additionalDBParams.putAll(dbParams);
        }
        return getThis();
    }

    public T options(Map<String, String> params) {
        additionalRequestParams = new HashMap<>();
        if (null != params) {
            additionalRequestParams.putAll(params);
        }
        return getThis();
    }

    public T option(String key, String value) {
        additionalRequestParams.put(key, value);
        return getThis();
    }

    public T removeOption(String key) {
        additionalRequestParams.remove(key);
        return getThis();
    }

    protected abstract T getThis();

}
