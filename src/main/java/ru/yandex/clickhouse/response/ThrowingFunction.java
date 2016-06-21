package ru.yandex.clickhouse.response;

public interface ThrowingFunction<F,R> {
    R apply(F f) throws Exception;
}