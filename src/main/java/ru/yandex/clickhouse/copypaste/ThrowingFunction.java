package ru.yandex.clickhouse.copypaste;

public interface ThrowingFunction<F,R> {
    R apply(F f) throws Exception;
}