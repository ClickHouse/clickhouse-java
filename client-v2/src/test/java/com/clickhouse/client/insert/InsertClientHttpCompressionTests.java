package com.clickhouse.client.insert;

public class InsertClientHttpCompressionTests extends InsertTests {

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }

    public InsertClientHttpCompressionTests() {
        super(true, true);
    }
}
