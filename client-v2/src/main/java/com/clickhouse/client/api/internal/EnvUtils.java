package com.clickhouse.client.api.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.NetworkInterface;

/**
 * Environment utility class.
 */
public class EnvUtils {

    private static final Logger LOG = LoggerFactory.getLogger(EnvUtils.class);

    /**
     * Returns the local host name or address.
     * @param returnAddress if true, return address; otherwise, return name
     * @return string representing the local host name or address
     */
    public static String getLocalhostNameOrAddress(final boolean returnAddress) {
        try {

            return NetworkInterface.networkInterfaces()
                    .flatMap(NetworkInterface::inetAddresses)
                    .filter(ia -> !ia.isLoopbackAddress())
                    .findFirst()
                    .map(ia -> returnAddress ? ia.getHostAddress() : ia.getCanonicalHostName())
                    .orElse("");
        } catch (Exception e) {
            LOG.error("Failed to get local host name or address", e);
        }
        return "";
    }
}
