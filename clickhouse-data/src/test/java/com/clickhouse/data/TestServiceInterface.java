package com.clickhouse.data;

public interface TestServiceInterface {
    default String getValue() {
        return TestServiceInterface.class.getSimpleName();
    }
}
