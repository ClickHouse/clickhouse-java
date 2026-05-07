package com.clickhouse.client.api.data_formats.internal;

import java.io.InputStream;
import java.lang.reflect.Constructor;

public class JsonParserFactory {
    public static JsonParser createParser(String type, InputStream inputStream) {
        String className;
        if ("JACKSON".equalsIgnoreCase(type)) {
            className = "com.clickhouse.client.api.data_formats.internal.JacksonJsonParser";
        } else if ("GSON".equalsIgnoreCase(type)) {
            className = "com.clickhouse.client.api.data_formats.internal.GsonJsonParser";
        } else {
            throw new IllegalArgumentException("Unsupported JSON processor: " + type + ". Supported: JACKSON, GSON");
        }

        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor(InputStream.class);
            return (JsonParser) constructor.newInstance(inputStream);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JSON processor class not found: " + className + ". Make sure you have the required library (Jackson or Gson) on your classpath.", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate JSON processor: " + type, e);
        }
    }
}
