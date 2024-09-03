package com.clickhouse.client.insert;

public class InsertClientContentCompressionTests extends InsertTests {

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }

    public InsertClientContentCompressionTests() {
        super(true, false);
    }
}
