package ru.yandex.clickhouse.domain;

public enum ClickHouseCompression {
    none,
    gzip,
    brotli,
    deflate,
    zstd;
}
