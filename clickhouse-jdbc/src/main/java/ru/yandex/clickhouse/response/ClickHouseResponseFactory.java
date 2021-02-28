package ru.yandex.clickhouse.response;

import ru.yandex.clickhouse.Jackson;
import ru.yandex.clickhouse.ResponseFactory;

import java.io.IOException;
import java.io.InputStream;

public class ClickHouseResponseFactory implements ResponseFactory<ClickHouseResponse> {
    @Override
    public ClickHouseResponse create(InputStream response) throws IOException {
        return Jackson.getObjectMapper().readValue(response, ClickHouseResponse.class);
    }
}
