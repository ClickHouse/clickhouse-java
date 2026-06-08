package com.clickhouse.client.api.internal;

/**
 * Class containing utility methods used across the client.
 */
public final class ClientUtils {

    private ClientUtils() {}

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }
}
