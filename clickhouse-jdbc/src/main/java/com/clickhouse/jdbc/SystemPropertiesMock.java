package com.clickhouse.jdbc;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class SystemPropertiesMock implements AutoCloseable {

    private Map<String, String> previousProperties = new HashMap<>();

    private SystemPropertiesMock(String...keyValues) {
        if (keyValues == null || keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Improper key-value pairs");
        }
        Map<String, String> mockProperties = new HashMap<>();

        for (int i=0; i<keyValues.length; i+=2) {
            String key = keyValues[i];
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("Key can not be blank");
            }
            previousProperties.put(key, System.getProperty(key));
            mockProperties.put(key, keyValues[i+1]);
        }
        setSystemProperties(mockProperties);
    }

    public static SystemPropertiesMock of(String...keyValues) {
        return new SystemPropertiesMock(keyValues);
    }

    @Override
    public void close() throws Exception {
        setSystemProperties(previousProperties);
    }

    private void setSystemProperties(Map<String, String> properties) {
        for (String key : properties.keySet()) {
            String value = properties.get(key);
            if (value == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, value);
            }
        }
    }

}
