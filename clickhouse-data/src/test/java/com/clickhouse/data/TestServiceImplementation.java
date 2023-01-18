package com.clickhouse.data;

public class TestServiceImplementation implements TestServiceInterface {
    @Override
    public String getValue() {
        return TestServiceImplementation.class.getSimpleName();
    }
}
