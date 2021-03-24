package ru.yandex.clickhouse;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for consuming content from server response and
 * @param <T> type of response
 */
public interface ResponseFactory<T> {

    public T create(InputStream response) throws IOException;
}
