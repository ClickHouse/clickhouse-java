package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import org.apache.hc.core5.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Builds the identification of the client and of the application that uses it. The identification is the value of
 * the {@code User-Agent} header, in the standard ClickHouse client format:
 * {@code <application> clickhouse-java-v2/<version> (<key>:<value>; ...; lv:java/<java.version>; os:<os.name>;
 * hc:ahc_<Apache HttpClient version>)}.
 * <p>
 * The application part is the client name ({@link ClientConfigProperties#CLIENT_NAME}), or the custom
 * {@code User-Agent} header if the application set one. The key/value pairs are the client metadata
 * ({@link ClientConfigProperties#CLIENT_ID_METADATA}). The {@code hc} entry is the HTTP client that sends the requests:
 * {@code ahc_} (Apache HttpClient) and its version.
 * <p>
 * The client metadata is validated when {@link Client.Builder#build()} parses the configuration. {@link Client}
 * creates one instance and updates it when the client name changes. The instance composes the value for the client
 * configuration at these times, and keeps it. A transport only sends the value of {@link #getUserAgent(String, Map)}
 * with every request.
 */
public final class WorkloadIdentitySupport {

    private static final Logger LOG = LoggerFactory.getLogger(WorkloadIdentitySupport.class);

    private static final String METADATA_KEY_LANGUAGE_VERSION = "lv";

    private static final String METADATA_KEY_OS = "os";

    private static final String METADATA_KEY_HTTP_CLIENT = "hc";

    /**
     * First and last characters of the printable ASCII range, which client metadata keys and values must be in.
     */
    private static final char START_OF_ASCII_RANGE = ' ';

    private static final char END_OF_ASCII_RANGE = '~';

    /**
     * Length of the {@code User-Agent} value above which the client logs a warning. Common browsers send about 120
     * characters.
     */
    private static final int USER_AGENT_WARNING_LENGTH = 200;

    private final String clientPart;

    private volatile String clientApplication;

    private volatile String clientUserAgent;

    /**
     * Composes the client part and the value of the {@code User-Agent} header for the client configuration.
     *
     * @param configuration - parsed client configuration
     */
    public WorkloadIdentitySupport(Map<String, Object> configuration) {
        this.clientPart = composeClientPart(configuration);
        updateClientName(configuration);
    }

    /**
     * Composes the value of the {@code User-Agent} header for the client configuration again, and keeps it. Call it
     * after the client name changes. Logs a warning if the value is longer than 200 characters.
     * <p>
     * Updates are synchronized, so the kept application part and value always belong together. A request that runs
     * at the same time as an update can get the value of the new client name.
     *
     * @param configuration - client configuration
     */
    public synchronized void updateClientName(Map<String, Object> configuration) {
        String application = applicationPart(
                (String) configuration.get(ClientConfigProperties.httpHeader(HttpHeaders.USER_AGENT)), configuration);
        String userAgent = composeUserAgent(application);
        if (userAgent.length() > USER_AGENT_WARNING_LENGTH) {
            LOG.warn("The User-Agent value is {} characters long, more than {}: '{}'. Use a shorter client name or"
                    + " less client metadata.", userAgent.length(), USER_AGENT_WARNING_LENGTH, userAgent);
        }
        // the value is written first, so a request that reads the new application part also reads the new value
        this.clientUserAgent = userAgent;
        this.clientApplication = application;
    }

    /**
     * Returns the value of the {@code User-Agent} header for a request: the application part, then the client part.
     * The application part is {@code customUserAgent} if the application set its own {@code User-Agent} header, and
     * the client name otherwise. Leading and trailing whitespace is removed. If the application part is blank or is
     * the one of the client configuration, the value that this instance keeps is returned. Otherwise, for example for
     * a {@code User-Agent} header of one operation, a new value is composed and not kept.
     *
     * @param customUserAgent - value of the {@code User-Agent} header that the application set, or {@code null}
     * @param requestConfig - settings of the request
     * @return value of the {@code User-Agent} header
     */
    public String getUserAgent(String customUserAgent, Map<String, Object> requestConfig) {
        String application = applicationPart(customUserAgent, requestConfig);
        return application.isEmpty() || application.equals(clientApplication)
                ? clientUserAgent : composeUserAgent(application);
    }

    /**
     * Returns the trimmed application part, or an empty string if it is not set.
     */
    private static String applicationPart(String customUserAgent, Map<String, Object> config) {
        String value = ClientUtils.isBlank(customUserAgent)
                ? ClientConfigProperties.CLIENT_NAME.<String>getOrDefault(config) : customUserAgent;
        return ClientUtils.isBlank(value) ? "" : value.trim();
    }

    private String composeUserAgent(String application) {
        return application.isEmpty() ? clientPart : application + " " + clientPart;
    }

    /**
     * Checks that a client metadata entry does not break the format of the {@code User-Agent} header.
     *
     * @param key - trimmed, not empty key
     * @param value - trimmed, not empty value
     * @throws ClientMisconfigurationException if the key or the value contains a character that is not allowed
     */
    public static void checkClientMetadataEntry(String key, String value) {
        // ':' separates a key from its value, ';' separates entries, '(' and ')' delimit the comment group,
        // and ',' and '=' are separators of the client_id_metadata property value
        if (!isClientMetadataText(key, " :;(),=") || !isClientMetadataText(value, ";()")) {
            throw new ClientMisconfigurationException("Invalid '" + ClientConfigProperties.CLIENT_ID_METADATA.getKey()
                    + "' entry '" + key + "=" + value + "': keys and values must be printable ASCII, a key must not"
                    + " contain spaces or any of ':;(),=', and a value must not contain any of ';()'");
        }
    }

    private static boolean isClientMetadataText(String str, String forbiddenChars) {
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (ch < START_OF_ASCII_RANGE || ch > END_OF_ASCII_RANGE || forbiddenChars.indexOf(ch) >= 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Composes the client part: {@code clickhouse-java-v2/<version> (<key>:<value>; ...)}. The key/value pairs are the
     * client metadata, if it is set, then {@code lv:java/<java.version>}, {@code os:<os.name>} and
     * {@code hc:ahc_<Apache HttpClient version>} if the client metadata does not set these keys (ignoring case). The
     * client sends requests over HTTP with Apache HttpClient ({@code ahc}).
     */
    private static String composeClientPart(Map<String, Object> configuration) {
        Map<String, String> metadata = new LinkedHashMap<>();
        ClientConfigProperties.CLIENT_ID_METADATA.<Map<String, String>>applyIfSet(configuration, metadata::putAll);
        ClientUtils.putIfAbsentIgnoreCase(metadata, METADATA_KEY_LANGUAGE_VERSION,
                "java/" + System.getProperty("java.version"));
        ClientUtils.putIfAbsentIgnoreCase(metadata, METADATA_KEY_OS, System.getProperty("os.name"));
        ClientUtils.putIfAbsentIgnoreCase(metadata, METADATA_KEY_HTTP_CLIENT, "ahc_" + Versions.APACHE_HTTP_CLIENT);

        StringJoiner comment = new StringJoiner("; ", Client.CLIENT_USER_AGENT + Client.clientVersion + " (", ")");
        metadata.forEach((key, value) -> comment.add(key + ":" + value));
        return comment.toString();
    }
}
