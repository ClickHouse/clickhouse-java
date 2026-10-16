package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;

import java.util.Map;

/**
 * Builds the identification of the client and of the application that uses it. The identification is the value of
 * the {@code User-Agent} header, in the standard ClickHouse client format:
 * {@code <application> clickhouse-java-v2/<version> (<key>:<value>; ...; lv:java/<java.version>; os:<os.name>)}.
 * <p>
 * The application part is the client name ({@link ClientConfigProperties#CLIENT_NAME}), and the key/value pairs are
 * the client metadata ({@link ClientConfigProperties#CLIENT_METADATA}). A transport sends the value of
 * {@link #getUserAgent(String, Map)} with every request, so the identification is the same for every transport.
 */
public class WorkloadIdentitySupport {

    private static final String RESERVED_KEY_LANGUAGE_VERSION = "lv";

    private static final String RESERVED_KEY_OS = "os";

    private final String clientUserAgent;

    /**
     * @param configuration - parsed client configuration
     */
    public WorkloadIdentitySupport(Map<String, Object> configuration) {
        this.clientUserAgent = buildClientUserAgent(ClientConfigProperties.CLIENT_METADATA.getOrDefault(configuration));
    }

    /**
     * Returns the value of the {@code User-Agent} header for a request: the application part, then the client part.
     * The application part is {@code customUserAgent} if the application set its own {@code User-Agent} header, and
     * the client name otherwise. A blank value counts as not set, and a blank application part is left out.
     *
     * @param customUserAgent - value of the {@code User-Agent} header that the application set, or {@code null}
     * @param requestConfig - settings of the request
     * @return value of the {@code User-Agent} header
     */
    public String getUserAgent(String customUserAgent, Map<String, Object> requestConfig) {
        String application = isBlank(customUserAgent)
                ? ClientConfigProperties.CLIENT_NAME.<String>getOrDefault(requestConfig) : customUserAgent;
        return isBlank(application) ? clientUserAgent : application + " " + clientUserAgent;
    }

    private static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    /**
     * Checks that a client metadata entry does not break the format of the {@code User-Agent} header.
     *
     * @param key - trimmed, not empty key
     * @param value - trimmed, not empty value
     * @throws ClientMisconfigurationException if the key is reserved, or the key or the value contains a character
     *         that is not allowed
     */
    public static void checkClientMetadataEntry(String key, String value) {
        if (key.equalsIgnoreCase(RESERVED_KEY_LANGUAGE_VERSION) || key.equalsIgnoreCase(RESERVED_KEY_OS)) {
            throw new ClientMisconfigurationException("Invalid '" + ClientConfigProperties.CLIENT_METADATA.getKey()
                    + "' entry '" + key + "': the key is reserved by the client");
        }
        if (!isClientMetadataText(key, " :;(),=") || !isClientMetadataText(value, ";()")) {
            throw new ClientMisconfigurationException("Invalid '" + ClientConfigProperties.CLIENT_METADATA.getKey()
                    + "' entry '" + key + "=" + value + "': keys and values must be printable ASCII, a key must not"
                    + " contain spaces or any of ':;(),=', and a value must not contain any of ';()'");
        }
    }

    private static boolean isClientMetadataText(String str, String forbiddenChars) {
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (ch < 0x20 || ch > 0x7E || forbiddenChars.indexOf(ch) >= 0) {
                return false;
            }
        }
        return true;
    }

    private static String buildClientUserAgent(Map<String, String> clientMetadata) {
        StringBuilder userAgent = new StringBuilder();
        userAgent.append(Client.CLIENT_USER_AGENT).append(Client.clientVersion).append(" (");
        if (clientMetadata != null) {
            for (Map.Entry<String, String> entry : clientMetadata.entrySet()) {
                userAgent.append(entry.getKey()).append(':').append(entry.getValue()).append("; ");
            }
        }
        userAgent.append(RESERVED_KEY_LANGUAGE_VERSION).append(":java/").append(System.getProperty("java.version"))
                .append("; ").append(RESERVED_KEY_OS).append(':').append(System.getProperty("os.name"))
                .append(')');
        return userAgent.toString();
    }
}
