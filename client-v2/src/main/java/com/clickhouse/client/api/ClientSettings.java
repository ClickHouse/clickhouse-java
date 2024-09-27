package com.clickhouse.client.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * All known client settings at current version.
 *
 */
public class ClientSettings {

    public static final String HTTP_HEADER_PREFIX = "http_header_";

    public static final String SERVER_SETTING_PREFIX = "clickhouse_setting_";

    public static String commaSeparated(Collection<?> values) {
        StringBuilder sb = new StringBuilder();
        for (Object value : values) {
            sb.append(value.toString().replaceAll(",", "\\,")).append(",");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public static List<String> valuesFromCommaSeparated(String value) {
        return Arrays.stream(value.split(",")).map(s -> s.replaceAll("\\\\,", ","))
                .collect(Collectors.toList());
    }
}
