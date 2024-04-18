package com.clickhouse.client.api.internal;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class SettingsConverter {

    public static Map<String, Serializable> toRequestSettings(Map<String, Object> settings) {
        Map<String, Serializable> requestSettings = new HashMap<>();

        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            if (entry.getValue() instanceof Map<?,?>) {
                Map<String, String> map = (Map<String, String>) entry.getValue();
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, String> e : map.entrySet()) {
                    sb.append(escape(e.getKey())).append('=').append(escape(e.getValue())).append(',');
                }
                requestSettings.put(entry.getKey(), sb.substring(0, sb.length() - 1));
            } else if (entry.getValue() instanceof Collection<?>) {
                Collection<?> collection = (Collection<?>) entry.getValue();
                StringBuilder sb = new StringBuilder();
                for (Object value : collection) {
                    sb.append(escape(value.toString())).append(',');
                }
                requestSettings.put(entry.getKey(), sb.substring(0, sb.length() - 1));
            } else {
                if (entry.getKey().equals("format")) {
                    continue;
                }
                requestSettings.put(entry.getKey(), (Serializable) entry.getValue());
            }
        }

        return requestSettings;
    }


    private static final Pattern ESCAPE_PATTERN = Pattern.compile("[,'\\\"=\\t\\n]{1}");

    public static String escape(String value) {
        return ESCAPE_PATTERN.matcher(value).replaceAll("\\\\$0");
    }
}
