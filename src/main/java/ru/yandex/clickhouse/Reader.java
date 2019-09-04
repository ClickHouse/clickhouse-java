package ru.yandex.clickhouse;

import java.util.Collections;
import java.util.Map;

class Reader extends ConfigurableApi<Reader> {

    private Map<String, String> additionalRequestParams = Collections.emptyMap();

    Reader(ClickHouseStatementImpl statement) {
        super(statement);
    }

    public Reader withRequestParams(Map<String, String> params) {
        this.additionalRequestParams = null == params ? Collections.<String, String>emptyMap() : params;
        return this;
    }
}
