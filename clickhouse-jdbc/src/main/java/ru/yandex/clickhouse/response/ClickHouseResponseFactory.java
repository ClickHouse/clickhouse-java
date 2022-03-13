package ru.yandex.clickhouse.response;

import ru.yandex.clickhouse.ResponseFactory;

import java.io.IOException;
import java.io.InputStream;

import com.clickhouse.client.data.JsonStreamUtils;

public class ClickHouseResponseFactory implements ResponseFactory<ClickHouseResponse> {
    @Override
    public ClickHouseResponse create(InputStream response) throws IOException {
        return JsonStreamUtils.readObject(response, ClickHouseResponse.class);
    }
}
