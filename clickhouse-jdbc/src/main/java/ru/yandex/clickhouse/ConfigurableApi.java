package ru.yandex.clickhouse;

import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.util.*;

@SuppressWarnings("unchecked")
class ConfigurableApi<T> {

    protected final ClickHouseStatementImpl statement;
    private Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<ClickHouseQueryParam, String>();
    private Map<String, String> additionalRequestParams = new HashMap<String, String>();
    private List<ClickHouseExternalData> additionalExternalData = new ArrayList<>();

    ConfigurableApi(ClickHouseStatementImpl statement) {
        this.statement = statement;
    }

    Map<String, String> getRequestParams() {
        return additionalRequestParams;
    }

    Map<ClickHouseQueryParam, String> getAdditionalDBParams() {
        return additionalDBParams;
    }

    List<ClickHouseExternalData> getAdditionalExternalData() {
        return additionalExternalData;
    }

    public T addDbParam(ClickHouseQueryParam param, String value) {
        additionalDBParams.put(param, value);
        return (T) this;
    }

    public T removeDbParam(ClickHouseQueryParam param) {
        additionalDBParams.remove(param);
        return (T) this;
    }

    public T withDbParams(Map<ClickHouseQueryParam, String> dbParams) {
        this.additionalDBParams = new HashMap<ClickHouseQueryParam, String>();
        if (null != dbParams) {
            additionalDBParams.putAll(dbParams);
        }
        return (T) this;
    }

    public T options(Map<String, String> params) {
        additionalRequestParams = new HashMap<String, String>();
        if (null != params) {
            additionalRequestParams.putAll(params);
        }
        return (T) this;
    }

    public T option(String key, String value) {
        additionalRequestParams.put(key, value);
        return (T) this;
    }

    public T removeOption(String key) {
        additionalRequestParams.remove(key);
        return (T) this;
    }

    public T addExternalData(ClickHouseExternalData data) {
        additionalExternalData.add(data);
        return (T) this;
    }

    public T withExternalData(List<ClickHouseExternalData> data) {
        additionalExternalData = new ArrayList<>();
        if (data != null) {
            additionalExternalData.addAll(data);
        }
        return (T) this;
    }
}
