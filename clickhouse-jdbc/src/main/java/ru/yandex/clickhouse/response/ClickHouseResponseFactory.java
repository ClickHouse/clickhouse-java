package ru.yandex.clickhouse.response;

import ru.yandex.clickhouse.ResponseFactory;
import ru.yandex.clickhouse.util.JsonStreamUtilsLegacy;

import java.io.IOException;
import java.io.InputStream;

public class ClickHouseResponseFactory implements ResponseFactory<ClickHouseResponse> {
    @Override
    public ClickHouseResponse create(InputStream response) throws IOException {
        return JsonStreamUtilsLegacy.readObject(response, ClickHouseResponse.class);
    }
}
