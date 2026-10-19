package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Builds the identification of the client and of the application that uses it. The identification is the value of
 * the {@code User-Agent} header, in the standard ClickHouse client format:
 * {@code <application> clickhouse-java-v2/<version> (<key>:<value>; ...; lv:java/<java.version>; os:<os.name>)}.
 * <p>
 * The application part is the client name ({@link ClientConfigProperties#CLIENT_NAME}), and the key/value pairs are
 * the client metadata ({@link ClientConfigProperties#CLIENT_METADATA}). A transport sends the value of
 * {@link #getUserAgent(String, Map)} with every request, so the identification is the same for every transport.
 * <p>
 * The value is composed once and cached. The mandatory part is the same for every client and is composed when the
 * class is initialized. The client metadata is validated when {@link Client.Builder#build()} creates the client and
 * parses its configuration, and is composed once per client. The complete value is composed again only when the
 * application part changes.
 */
public class WorkloadIdentitySupport {

    private static final String RESERVED_KEY_LANGUAGE_VERSION = "lv";

    private static final String RESERVED_KEY_OS = "os";

    /** Start of the client part, up to the comment group. */
    private static final String CLIENT_PART_START = Client.CLIENT_USER_AGENT + Client.clientVersion + " (";

    /** Mandatory end of the comment group. */
    private static final String CLIENT_PART_END = RESERVED_KEY_LANGUAGE_VERSION + ":java/"
            + System.getProperty("java.version") + "; " + RESERVED_KEY_OS + ":" + System.getProperty("os.name") + ")";

    /** Client part when there is no client metadata. */
    private static final String CLIENT_PART_WITHOUT_METADATA = CLIENT_PART_START + CLIENT_PART_END;

    private final String clientPart;

    private final AtomicReference<UserAgent> lastUserAgent = new AtomicReference<>();

    /**
     * Composes the client part, and the complete value for the client name of the configuration.
     *
     * @param configuration - parsed client configuration
     */
    public WorkloadIdentitySupport(Map<String, Object> configuration) {
        this.clientPart = composeClientPart(ClientConfigProperties.CLIENT_METADATA.getOrDefault(configuration));
        String application = applicationPart(null, configuration);
        this.lastUserAgent.set(new UserAgent(application, compose(application)));
    }

    /**
     * Returns the value of the {@code User-Agent} header for a request: the application part, then the client part.
     * The application part is {@code customUserAgent} if the application set its own {@code User-Agent} header, and
     * the client name otherwise. Leading and trailing whitespace is removed, a blank value counts as not set, and a blank
     * application part is left out. The value is cached, so it is composed again only when the application part
     * changes.
     *
     * @param customUserAgent - value of the {@code User-Agent} header that the application set, or {@code null}
     * @param requestConfig - settings of the request
     * @return value of the {@code User-Agent} header
     */
    public String getUserAgent(String customUserAgent, Map<String, Object> requestConfig) {
        String application = applicationPart(customUserAgent, requestConfig);
        UserAgent userAgent = lastUserAgent.get();
        if (!application.equals(userAgent.application)) {
            userAgent = new UserAgent(application, compose(application));
            lastUserAgent.set(userAgent);
        }
        return userAgent.value;
    }

    /**
     * Returns the trimmed application part, or an empty string if it is not set.
     */
    private static String applicationPart(String customUserAgent, Map<String, Object> config) {
        String application = ClientUtils.isBlank(customUserAgent)
                ? ClientConfigProperties.CLIENT_NAME.<String>getOrDefault(config) : customUserAgent;
        return ClientUtils.isBlank(application) ? "" : application.trim();
    }

    private String compose(String application) {
        return application.isEmpty() ? clientPart : application + " " + clientPart;
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

    private static String composeClientPart(Map<String, String> clientMetadata) {
        if (clientMetadata == null || clientMetadata.isEmpty()) {
            return CLIENT_PART_WITHOUT_METADATA;
        }
        StringBuilder clientPart = new StringBuilder(CLIENT_PART_START);
        for (Map.Entry<String, String> entry : clientMetadata.entrySet()) {
            clientPart.append(entry.getKey()).append(':').append(entry.getValue()).append("; ");
        }
        return clientPart.append(CLIENT_PART_END).toString();
    }

    /**
     * Complete value of the {@code User-Agent} header for one application part.
     */
    private static final class UserAgent {

        private final String application;

        private final String value;

        private UserAgent(String application, String value) {
            this.application = application;
            this.value = value;
        }
    }
}
