package com.clickhouse.client.api.internal;

import org.slf4j.Logger;

import java.io.Closeable;

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

    public static void quietClose(Closeable closeable, Logger log) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.warn("Failed to close object " + closeable, e);
            }
        }
    }
}
