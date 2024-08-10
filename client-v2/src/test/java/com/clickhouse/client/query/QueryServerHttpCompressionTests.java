package com.clickhouse.client.query;

public class QueryServerHttpCompressionTests extends QueryTests {
    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }
    QueryServerHttpCompressionTests() {
        super(true, true);
    }
}
