package com.clickhouse.client.api.data_formats;


import java.util.HashMap;
import java.util.Map;

/**
 * Base class for all supported data formats.
 * See <a href="https://clickhouse.com/docs/en/interfaces/formats">List of supported data formats</a>
 */
public class DataFormat {


    private Map<String, String> settings = new HashMap<>();

    protected DataFormat() {

    }

    public void setSetting(String key, String value) {
        settings.put(key, value);
    }
    public String getSetting(String key) {
        return settings.get(key);
    }
    public Map<String, String> getSettings() {
        return settings;
    }
    public String removeSetting(String key) {
        return settings.remove(key);
    }
}
