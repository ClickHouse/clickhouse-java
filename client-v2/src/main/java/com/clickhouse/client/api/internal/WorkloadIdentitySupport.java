package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import org.apache.hc.core5.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.StringJoiner;

/**
 * Builds the identification of the client and of the application that uses it. The identification is the value of
 * the {@code User-Agent} header, in the standard ClickHouse client format:
 * {@code <application> clickhouse-java-v2/<version> (<key>:<value>; ...; lv:java/<java.version>; os:<os.name>)}.
 * <p>
 * The application part is the client name ({@link ClientConfigProperties#CLIENT_NAME}), or the custom
 * {@code User-Agent} header if the application set one. The key/value pairs are the client metadata
 * ({@link ClientConfigProperties#CLIENT_METADATA}). A transport sends the value of
 * {@link #getUserAgent(String, Map)} with every request, so the identification is the same for every transport.
 * <p>
 * The client metadata is validated when {@link Client.Builder#build()} parses the configuration. An instance composes
 * the value for the client configuration once and keeps it. When the client name changes, the client creates a new
 * instance.
 */
public class WorkloadIdentitySupport {

    private static final Logger LOG = LoggerFactory.getLogger(WorkloadIdentitySupport.class);

    private static final String RESERVED_KEY_LANGUAGE_VERSION = "lv";

    private static final String RESERVED_KEY_OS = "os";

    /**
     * Length of the {@code User-Agent} value above which the client logs a warning. Common browsers send about 120
     * characters.
     */
    private static final int USER_AGENT_WARNING_LENGTH = 200;

    private final String clientPart;

    private final String clientApplication;

    private final String clientUserAgent;

    /**
     * Composes the value of the {@code User-Agent} header for the client configuration, and logs a warning if the
     * value is longer than 200 characters.
     *
     * @param configuration - parsed client configuration
     */
    public WorkloadIdentitySupport(Map<String, Object> configuration) {
        this.clientPart = composeClientPart(configuration);
        this.clientApplication = applicationPart(
                (String) configuration.get(ClientConfigProperties.httpHeader(HttpHeaders.USER_AGENT)), configuration);
        this.clientUserAgent = compose(clientApplication);
        if (clientUserAgent.length() > USER_AGENT_WARNING_LENGTH) {
            LOG.warn("The User-Agent value is {} characters long, more than {}: '{}'. Use a shorter client name or"
                    + " less client metadata.", clientUserAgent.length(), USER_AGENT_WARNING_LENGTH, clientUserAgent);
        }
    }

    /**
     * Returns the value of the {@code User-Agent} header for a request: the application part, then the client part.
     * The application part is {@code customUserAgent} if the application set its own {@code User-Agent} header, and
     * the client name otherwise. Leading and trailing whitespace is removed, a blank value counts as not set, and a blank
     * application part is left out. If the application part is the one of the client configuration, the value that
     * this instance keeps is returned. Otherwise, for example for a {@code User-Agent} header of one operation, a new
     * value is composed and not kept.
     *
     * @param customUserAgent - value of the {@code User-Agent} header that the application set, or {@code null}
     * @param requestConfig - settings of the request
     * @return value of the {@code User-Agent} header
     */
    public String getUserAgent(String customUserAgent, Map<String, Object> requestConfig) {
        String application = applicationPart(customUserAgent, requestConfig);
        return application.equals(clientApplication) ? clientUserAgent : compose(application);
    }

    /**
     * Returns the trimmed application part, or an empty string if it is not set.
     */
    private static String applicationPart(String customUserAgent, Map<String, Object> config) {
        String value = ClientUtils.isBlank(customUserAgent)
                ? ClientConfigProperties.CLIENT_NAME.<String>getOrDefault(config) : customUserAgent;
        return ClientUtils.isBlank(value) ? "" : value.trim();
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
        // ':' separates a key from its value, ';' separates entries, '(' and ')' delimit the comment group,
        // and ',' and '=' are separators of the client_metadata property value
        if (!isClientMetadataText(key, " :;(),=") || !isClientMetadataText(value, ";()")) {
            throw new ClientMisconfigurationException("Invalid '" + ClientConfigProperties.CLIENT_METADATA.getKey()
                    + "' entry '" + key + "=" + value + "': keys and values must be printable ASCII, a key must not"
                    + " contain spaces or any of ':;(),=', and a value must not contain any of ';()'");
        }
    }

    private static boolean isClientMetadataText(String str, String forbiddenChars) {
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (!isPrintableAscii(ch) || forbiddenChars.indexOf(ch) >= 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns {@code true} if the character is printable ASCII: from the space character to {@code '~'}.
     */
    private static boolean isPrintableAscii(char ch) {
        return ch >= ' ' && ch <= '~';
    }

    /**
     * Composes the client part: {@code clickhouse-java-v2/<version> (<key>:<value>; ...; lv:java/<java.version>;
     * os:<os.name>)}. The key/value pairs are the client metadata, if it is set.
     */
    private static String composeClientPart(Map<String, Object> configuration) {
        StringJoiner comment = new StringJoiner("; ", Client.CLIENT_USER_AGENT + Client.clientVersion + " (", ")");
        ClientConfigProperties.CLIENT_METADATA.<Map<String, String>>applyIfSet(configuration,
                metadata -> metadata.forEach((key, value) -> comment.add(key + ":" + value)));
        comment.add(RESERVED_KEY_LANGUAGE_VERSION + ":java/" + System.getProperty("java.version"));
        comment.add(RESERVED_KEY_OS + ":" + System.getProperty("os.name"));
        return comment.toString();
    }
}
