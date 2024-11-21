package com.clickhouse.client.api.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;

/**
 * Environment utility class.
 */
public class EnvUtils {

    private static final Logger LOG = LoggerFactory.getLogger(EnvUtils.class);

    /**
     * Returns the local host name or IP address. Can be used to set {@code Referer} HTTP header.
     * If fails to find the local host name or address, returns an empty string.
     * @param returnAddress if true, return address; otherwise, return name
     * @return string representing the local host name or address
     */
    public static String getLocalhostNameOrAddress(final boolean returnAddress) {
        try {

            for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                for (InetAddress inetAddress : Collections.list(networkInterface.getInetAddresses())) {
                    if (inetAddress.isLoopbackAddress()) {
                        continue;
                    }
                    if (returnAddress) {
                        return inetAddress.getHostAddress();
                    } else {
                        return inetAddress.getCanonicalHostName();
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get local host name or address", e);
        }
        return "";
    }
}
