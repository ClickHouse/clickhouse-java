package com.clickhouse.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.WeakHashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * This class depicts a ClickHouse server, essentially a combination of host,
 * port and protocol, for client to connect.
 */
@Deprecated
public class ClickHouseNode implements Function<ClickHouseNodeSelector, ClickHouseNode>, Serializable {
    /**
     * Node status.
     */
    public enum Status {
        /**
         * Healthy status.
         */
        HEALTHY,
        /**
         * Managed status.
         */
        MANAGED,
        /**
         * Faulty status.
         */
        FAULTY,
        /**
         * Standalone status.
         */
        STANDALONE;
    }

    /**
     * Mutable and non-thread safe builder.
     */
    public static class Builder {
        protected String host;
        protected ClickHouseProtocol protocol;
        protected Integer port;
        protected ClickHouseCredentials credentials;

        protected final Map<String, String> options;
        // label is more expressive, but is slow for comparison
        protected final Set<String> tags;

        /**
         * Default constructor.
         */
        protected Builder() {
            this.host = null;
            this.protocol = null;
            this.port = null;
            this.credentials = null;
            this.options = new LinkedHashMap<>();
            this.tags = new LinkedHashSet<>();
        }

        protected String getHost() {
            if (ClickHouseChecker.isNullOrEmpty(host)) {
                host = (String) ClickHouseDefaults.HOST.getEffectiveDefaultValue();
            }
            return host;
        }

        protected ClickHouseProtocol getProtocol() {
            if (protocol == null) {
                protocol = (ClickHouseProtocol) ClickHouseDefaults.PROTOCOL.getEffectiveDefaultValue();
            }
            return protocol;
        }

        protected int getPort() {
            if (port == null) {
                port = getProtocol().getDefaultPort();
            }
            return port;
        }

        protected ClickHouseCredentials getCredentials() {
            return credentials;
        }

        protected Map<String, String> getOptions() {
            return options;
        }

        protected Set<String> getTags() {
            return tags;
        }

        /**
         * Sets cluster name.
         *
         * @param cluster cluster name, null means no cluster name
         * @return this builder
         */
        public Builder cluster(String cluster) {
            if (!ClickHouseChecker.isNullOrEmpty(cluster)) {
                options.put(PARAM_CLUSTER, cluster);
            } else {
                options.remove(PARAM_CLUSTER);
            }
            return this;
        }

        /**
         * Sets host name.
         *
         * @param host host name, null means {@link ClickHouseDefaults#HOST}
         * @return this builder
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets port number.
         *
         * @param port port number, null means default port of
         *             {@link ClickHouseDefaults#PROTOCOL}
         * @return this builder
         */
        public Builder port(Integer port) {
            return port(null, port);
        }

        /**
         * Sets protocol used by the port.
         *
         * @param protocol protocol, null means {@link ClickHouseDefaults#PROTOCOL}
         * @return this builder
         */
        public Builder port(ClickHouseProtocol protocol) {
            return port(protocol, null);
        }

        /**
         * Sets protocol and port number.
         *
         * @param protocol protocol, null means {@link ClickHouseDefaults#PROTOCOL}
         * @param port     number, null means default port of {@code protocol}
         * @return this builder
         */
        public Builder port(ClickHouseProtocol protocol, Integer port) {
            this.protocol = protocol;
            this.port = port;
            return this;
        }

        /**
         * Sets socket address.
         *
         * @param address socket address, null means {@link ClickHouseDefaults#HOST} and
         *                default port of {@link ClickHouseDefaults#PROTOCOL}
         * @return this builder
         */
        public Builder address(InetSocketAddress address) {
            return address(null, address);
        }

        /**
         * Sets protocol and socket address.
         *
         * @param protocol protocol, null means {@link ClickHouseDefaults#PROTOCOL}
         * @param address  socket address, null means {@link ClickHouseDefaults#HOST}
         *                 and default port of {@code protocol}
         * @return this builder
         */
        public Builder address(ClickHouseProtocol protocol, InetSocketAddress address) {
            if (address != null) {
                host(address.getHostName());
                port(protocol, address.getPort());
            } else {
                host(null);
                port(protocol, null);
            }
            return this;
        }

        /**
         * Sets database name.
         *
         * @param database database name, null means {@link ClickHouseDefaults#DATABASE}
         * @return this builder
         */
        public Builder database(String database) {
            if (!ClickHouseChecker.isNullOrEmpty(database)) {
                options.put(ClickHouseClientOption.DATABASE.getKey(), database);
            } else {
                options.remove(ClickHouseClientOption.DATABASE.getKey());
            }
            return this;
        }

        /**
         * Sets credentials will be used when connecting to this node. This is optional
         * as client will use {@link ClickHouseConfig#getDefaultCredentials()} to
         * connect to all nodes.
         *
         * @param credentials credentials, null means
         *                    {@link ClickHouseConfig#getDefaultCredentials()}
         * @return this builder
         */
        public Builder credentials(ClickHouseCredentials credentials) {
            this.credentials = credentials;
            return this;
        }

        /**
         * Adds an option for this node.
         *
         * @param option option name, null value will be ignored
         * @param value  option value
         * @return this builder
         */
        public Builder addOption(String option, String value) {
            if (option != null) {
                if (value != null) {
                    options.put(option, value);
                } else {
                    options.remove(option);
                }
            }

            return this;
        }

        /**
         * Removes an option from this node.
         *
         * @param option option to be removed, null value will be ignored
         * @return this builder
         */
        public Builder removeOption(String option) {
            if (!ClickHouseChecker.isNullOrEmpty(option)) {
                options.remove(option);
            }

            return this;
        }

        /**
         * Sets all options for this node. Use null or empty value to clear all existing
         * options.
         *
         * @param options options for the node
         * @return this builder
         */
        public Builder options(Map<String, String> options) {
            this.options.clear();

            if (options != null) {
                this.options.putAll(options);
            }
            return this;
        }

        /**
         * Adds a tag for this node.
         *
         * @param tag tag for the node, null or duplicate tag will be ignored
         * @return this builder
         */
        public Builder addTag(String tag) {
            if (!ClickHouseChecker.isNullOrEmpty(tag)) {
                tags.add(tag);
            }

            return this;
        }

        /**
         * Removes a tag from this node.
         *
         * @param tag tag to be removed, null value will be ignored
         * @return this builder
         */
        public Builder removeTag(String tag) {
            if (!ClickHouseChecker.isNullOrEmpty(tag)) {
                tags.remove(tag);
            }

            return this;
        }

        /**
         * Sets all tags for this node. Use null or empty value to clear all existing
         * tags.
         *
         * @param tag  tag for the node, null will be ignored
         * @param more more tags for the node, null tag will be ignored
         * @return this builder
         */
        public Builder tags(String tag, String... more) {
            this.tags.clear();

            addTag(tag);

            if (more != null) {
                for (String t : more) {
                    addTag(t);
                }
            }

            return this;
        }

        /**
         * Sets all tags for this node. Use null or empty value to clear all existing
         * tags.
         *
         * @param tags list of tags for the node, null tag will be ignored
         * @return this builder
         */
        public Builder tags(Collection<String> tags) {
            this.tags.clear();

            if (tags != null) {
                for (String t : tags) {
                    addTag(t);
                }
            }

            return this;
        }

        public Builder replica(Integer num) {
            if (num != null) {
                options.put(PARAM_REPLICA_NUM, num.toString());
            } else {
                options.remove(PARAM_REPLICA_NUM);
            }
            return this;
        }

        public Builder shard(Integer num, Integer weight) {
            if (num != null) {
                options.put(PARAM_SHARD_NUM, num.toString());
            } else {
                options.remove(PARAM_SHARD_NUM);
            }
            if (weight != null) {
                options.put(PARAM_SHARD_WEIGHT, weight.toString());
            } else {
                options.remove(PARAM_SHARD_WEIGHT);
            }
            return this;
        }

        /**
         * Sets weight of this node.
         *
         * @param weight weight of the node, null means default weight
         * @return this builder
         */
        public Builder weight(Integer weight) {
            if (weight != null) {
                options.put(PARAM_WEIGHT, weight.toString());
            } else {
                options.remove(PARAM_WEIGHT);
            }
            return this;
        }

        /**
         * Sets time zone of this node.
         *
         * @param tz time zone ID, could be null
         * @return this builder
         */
        public Builder timeZone(String tz) {
            if (!ClickHouseChecker.isNullOrEmpty(tz)) {
                options.put(ClickHouseClientOption.SERVER_TIME_ZONE.getKey(), tz);
            } else {
                options.remove(ClickHouseClientOption.SERVER_TIME_ZONE.getKey());
            }
            return this;
        }

        /**
         * Sets time zone of this node.
         *
         * @param tz time zone, could be null
         * @return this builder
         */
        public Builder timeZone(TimeZone tz) {
            timeZone(tz != null ? tz.getID() : null);
            return this;
        }

        /**
         * Sets vesion of this node.
         *
         * @param version version string, could be null
         * @return this builder
         */
        public Builder version(String version) {
            if (!ClickHouseChecker.isNullOrEmpty(version)) {
                options.put(ClickHouseClientOption.SERVER_VERSION.getKey(), version);
            } else {
                options.remove(ClickHouseClientOption.SERVER_VERSION.getKey());
            }
            return this;
        }

        /**
         * Sets vesion of this node.
         *
         * @param version version, could be null
         * @return this builder
         */
        public Builder version(ClickHouseVersion version) {
            version(version != null ? version.toString() : null);
            return this;
        }

        /**
         * Creates a new node.
         *
         * @return new node
         */
        public ClickHouseNode build() {
            return new ClickHouseNode(this);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClickHouseNode.class);
    private static final long serialVersionUID = 8342604784121795372L;
    private static final Map<String, URI> cache = Collections.synchronizedMap(new WeakHashMap<>());

    static final ClickHouseNode DEFAULT = new ClickHouseNode("localhost", ClickHouseProtocol.ANY, 0, null, null, null);

    static final int MAX_PORT_NUM = 65535;
    static final int MIN_PORT_NUM = 0;

    static final String PARAM_CLUSTER = "cluster";
    static final String PARAM_SHARD_NUM = "shard_num";
    static final String PARAM_SHARD_WEIGHT = "shard_weight";
    static final String PARAM_REPLICA_NUM = "replica_num";
    static final String PARAM_WEIGHT = "weight";

    static final int DEFAULT_SHARD_NUM = 0;
    static final int DEFAULT_SHARD_WEIGHT = 0;
    static final int DEFAULT_REPLICA_NUM = 0;
    static final int DEFAULT_WEIGHT = 1;

    public static final String SCHEME_DELIMITER = "://";

    static int extract(String scheme, int port, ClickHouseProtocol protocol, Map<String, String> params) {
        if (port < MIN_PORT_NUM || port > MAX_PORT_NUM) {
            port = MIN_PORT_NUM;
        }
        if (protocol != ClickHouseProtocol.POSTGRESQL && scheme.charAt(scheme.length() - 1) == 's') {
            params.putIfAbsent(ClickHouseClientOption.SSL.getKey(), Boolean.TRUE.toString());
            params.putIfAbsent(ClickHouseClientOption.SSL_MODE.getKey(), ClickHouseSslMode.STRICT.name());
        }

        if (protocol != ClickHouseProtocol.ANY && port == MIN_PORT_NUM) {
            if (Boolean.TRUE.toString().equals(params.get(ClickHouseClientOption.SSL.getKey()))) {
                port = protocol.getDefaultSecurePort();
            } else {
                port = protocol.getDefaultPort();
            }
        }
        return port;
    }

    static ClickHouseCredentials extract(String rawUserInfo, Map<String, String> params,
            ClickHouseCredentials defaultCredentials) {
        ClickHouseCredentials credentials = defaultCredentials;
        String user = "";
        String passwd = "";
        if (credentials != null && !credentials.useAccessToken()) {
            user = credentials.getUserName();
            passwd = credentials.getPassword();
        }

        if (!ClickHouseChecker.isNullOrEmpty(rawUserInfo)) {
            int index = rawUserInfo.indexOf(':');
            if (index < 0) {
                user = ClickHouseUtils.decode(rawUserInfo);
            } else {
                String str = ClickHouseUtils.decode(rawUserInfo.substring(0, index));
                if (!ClickHouseChecker.isNullOrEmpty(str)) {
                    user = str;
                }
                passwd = ClickHouseUtils.decode(rawUserInfo.substring(index + 1));
            }
        }

        String str = params.remove(ClickHouseDefaults.USER.getKey());
        if (!ClickHouseChecker.isNullOrEmpty(str)) {
            user = str;
        }
        str = params.remove(ClickHouseDefaults.PASSWORD.getKey());
        if (str != null) {
            passwd = str;
        }
        if (!ClickHouseChecker.isNullOrEmpty(user)) {
            credentials = ClickHouseCredentials.fromUserAndPassword(user, passwd);
        } else if (!ClickHouseChecker.isNullOrEmpty(passwd)) {
            credentials = ClickHouseCredentials
                    .fromUserAndPassword((String) ClickHouseDefaults.USER.getEffectiveDefaultValue(), passwd);
        }
        return credentials;
    }

    static URI normalize(String uri, ClickHouseProtocol defaultProtocol) {
        int index = ClickHouseChecker.nonEmpty(uri, "URI").indexOf(SCHEME_DELIMITER);
        String normalized;
        if (index < 0) {
            normalized = new StringBuilder()
                    .append((defaultProtocol != null ? defaultProtocol : ClickHouseProtocol.ANY).name()
                            .toLowerCase(Locale.ROOT))
                    .append(SCHEME_DELIMITER)
                    .append(uri.trim()).toString();
        } else {
            normalized = uri.trim();
        }
        return cache.computeIfAbsent(normalized, k -> {
            try {
                return new URI(k);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid URI", e);
            }
        });
    }

    static void parseDatabase(String path, Map<String, String> params) {
        if (!ClickHouseChecker.isNullOrEmpty(path) && path.length() > 1) {
            params.put(ClickHouseClientOption.DATABASE.getKey(), path.substring(1));
        }
    }

    static void parseTags(String fragment, Set<String> tags) {
        if (ClickHouseChecker.isNullOrEmpty(fragment)) {
            return;
        }

        for (int i = 0, len = fragment.length(); i < len; i++) {
            int index = fragment.indexOf(',', i);
            if (index == i) {
                continue;
            }

            String tag;
            if (index < 0) {
                tag = fragment.substring(i).trim();
                i = len;
            } else {
                tag = fragment.substring(i, index).trim();
                i = index;
            }
            if (!tag.isEmpty()) {
                tags.add(tag);
            }
        }
    }

    static ClickHouseNode probe(String host, int port, ClickHouseSslMode mode, String rootCaFile, String certFile,
            String keyFile, int timeout) {
        if (mode == null) {
            return probe(host, port, timeout);
        }

        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        options.put(ClickHouseClientOption.SSL, true);
        options.put(ClickHouseClientOption.SSL_MODE, mode);
        options.put(ClickHouseClientOption.SSL_ROOT_CERTIFICATE, rootCaFile);
        options.put(ClickHouseClientOption.SSL_CERTIFICATE, certFile);
        options.put(ClickHouseClientOption.SSL_KEY, keyFile);
        ClickHouseConfig config = new ClickHouseConfig(options);
        SSLContext sslContext = null;
        try {
            sslContext = ClickHouseSslContextProvider.getProvider().getSslContext(SSLContext.class, config)
                    .orElse(null);
        } catch (SSLException e) {
            log.debug("Failed to create SSL context due to: %s", e.getMessage());
        }
        if (sslContext == null) {
            return probe(host, port, timeout);
        }

        SSLSocketFactory factory = sslContext.getSocketFactory();
        ClickHouseDnsResolver resolver = ClickHouseDnsResolver.getInstance();
        ClickHouseProtocol p = ClickHouseProtocol.HTTP;
        InetSocketAddress address = resolver != null
                ? resolver.resolve(ClickHouseProtocol.ANY, host, port)
                : new InetSocketAddress(host, port);
        try (SSLSocket client = (SSLSocket) factory.createSocket()) {
            client.setKeepAlive(false);
            client.setSoTimeout(timeout);
            client.connect(address, timeout);
            client.startHandshake();

            try (OutputStream out = client.getOutputStream(); InputStream in = client.getInputStream()) {
                out.write("GET /ping HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                byte[] buf = new byte[12]; // HTTP/1.x xxx
                int read = in.read(buf);
                if (read == -1) {
                    p = ClickHouseProtocol.GRPC;
                } else if (buf[0] == 72 && buf[9] == 52) {
                    p = ClickHouseProtocol.TCP;
                }
            }
        } catch (IOException e) {
            // MYSQL does not support SSL
            // and PostgreSQL will end up with SocketTimeoutException
            log.debug("Failed to probe %s:%d", host, port, e);
        }
        return new ClickHouseNode(host, p, port, null, null, null);
    }

    static ClickHouseNode probe(String host, int port, int timeout) {
        ClickHouseDnsResolver resolver = ClickHouseDnsResolver.getInstance();
        InetSocketAddress address = resolver != null
                ? resolver.resolve(ClickHouseProtocol.ANY, host, port)
                : new InetSocketAddress(host, port);

        ClickHouseProtocol p = ClickHouseProtocol.HTTP;
        // TODO needs a better way so that we can detect PostgreSQL port as well
        try (Socket client = new Socket()) {
            client.setKeepAlive(false);
            client.connect(address, timeout);
            client.setSoTimeout(timeout);
            try (OutputStream out = client.getOutputStream(); InputStream in = client.getInputStream()) {
                out.write("GET /ping HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                byte[] buf = new byte[12]; // HTTP/1.x xxx
                int read = in.read(buf);
                if (read == buf.length && buf[0] == 0) {
                    p = ClickHouseProtocol.GRPC;
                } else if (buf[0] != 0 && buf[3] == 0) {
                    p = ClickHouseProtocol.MYSQL;
                } else if (buf[0] == 72 && buf[9] == 52) {
                    p = ClickHouseProtocol.TCP;
                }
            }
        } catch (IOException e) {
            // PostgreSQL will end up with SocketTimeoutException
            log.debug("Failed to probe %s:%d", host, port, e);
        }

        return new ClickHouseNode(host, p, port, null, null, null);
    }

    static String value(String value, String defaultValue) {
        return ClickHouseChecker.isNullOrEmpty(value) ? defaultValue : value;
    }

    static int value(String value, int defaultValue) {
        return ClickHouseChecker.isNullOrEmpty(value) ? defaultValue : Integer.parseInt(value);
    }

    /**
     * Gets builder for creating a new node, same as {@code builder(null)}.
     *
     * @return builder for creating a new node
     */
    public static Builder builder() {
        return builder(null);
    }

    /**
     * Gets builder for creating a new node based on the given one.
     *
     * @param base template to start with
     * @return builder for creating a new node
     */
    public static Builder builder(ClickHouseNode base) {
        Builder b = new Builder();
        if (base != null) {
            Map<String, String> map = new LinkedHashMap<>(base.options);
            map.put(PARAM_CLUSTER, base.getCluster());
            map.put(PARAM_REPLICA_NUM, Integer.toString(base.replicaNum));
            map.put(PARAM_SHARD_NUM, Integer.toString(base.shardNum));
            map.put(PARAM_SHARD_WEIGHT, Integer.toString(base.shardWeight));
            map.put(PARAM_WEIGHT, Integer.toString(base.getWeight()));
            b.host(base.getHost()).port(base.getProtocol(), base.getPort()).credentials(base.credentials).options(map)
                    .tags(base.getTags());
        }
        return b;
    }

    /**
     * Creates a node object in one go. Short version of
     * {@code ClickHouseNode.builder().host(host).port(protocol, port).database(database).tags(tags).build()}.
     *
     * @param host     host name, null means {@link ClickHouseDefaults#HOST}
     * @param protocol protocol, null means {@link ClickHouseDefaults#PROTOCOL}
     * @param port     port number
     * @param database database name, null means {@link ClickHouseDefaults#DATABASE}
     * @param tags     tags for the node, null tag will be ignored
     * @return non-null node object
     */
    public static ClickHouseNode of(String host, ClickHouseProtocol protocol, int port, String database,
            String... tags) {
        Builder builder = builder().host(host).port(protocol, port).database(database);
        if (tags != null && tags.length > 0) {
            builder.tags(null, tags);
        }
        return builder.build();
    }

    /**
     * Creates a node object using given URI. Same as
     * {@code of(uri, ClickHouseProtocol.ANY)}.
     *
     * @param uri non-empty URI
     * @return non-null node object
     */
    public static ClickHouseNode of(String uri) {
        return of(uri, DEFAULT);
    }

    /**
     * Creates a node object using given URI. Same as
     * {@code of(uri, ClickHouseProtocol.ANY)}.
     *
     * @param uri     non-empty URI
     * @param options default options
     * @return non-null node object
     */
    public static ClickHouseNode of(String uri, Map<?, ?> options) {
        URI normalizedUri = normalize(uri, null);

        Map<String, String> params = new LinkedHashMap<>();
        parseDatabase(normalizedUri.getPath(), params);

        ClickHouseUtils.extractParameters(normalizedUri.getRawQuery(), params);

        Set<String> tags = new LinkedHashSet<>();
        parseTags(normalizedUri.getRawFragment(), tags);

        if (options != null && !options.isEmpty()) {
            for (Entry<?, ?> entry : options.entrySet()) {
                if (entry.getKey() != null) {
                    if (entry.getValue() != null) {
                        params.put(entry.getKey().toString(), entry.getValue().toString());
                    } else {
                        params.remove(entry.getKey().toString());
                    }
                }
            }
        }

        String scheme = normalizedUri.getScheme();
        ClickHouseProtocol protocol = ClickHouseProtocol.fromUriScheme(scheme);
        int port = extract(scheme, normalizedUri.getPort(), protocol, params);
        if ((options == null || options.get(ClickHouseClientOption.SSL.getKey()) == null)
                && scheme.equalsIgnoreCase("https")) {
            params.put(ClickHouseClientOption.SSL.getKey(), "true");
        }
        ClickHouseCredentials credentials = extract(normalizedUri.getRawUserInfo(), params, null);

        return new ClickHouseNode(normalizedUri.getHost(), protocol, port, credentials, params, tags);
    }

    /**
     * Creates a node object using given URI and template.
     *
     * @param uri      non-empty URI
     * @param template optinal template used for creating the node
     * @return non-null node object
     */
    public static ClickHouseNode of(String uri, ClickHouseNode template) {
        return of(normalize(uri, template != null ? template.getProtocol() : null), template);
    }

    /**
     * Creates a node object using given URI.
     *
     * @param uri      non-null URI which contains scheme(protocol), host and
     *                 optionally port
     * @param template optinal template used for creating the node object
     * @return non-null node object
     */
    public static ClickHouseNode of(URI uri, ClickHouseNode template) {
        String host = ClickHouseChecker.nonNull(uri, "URI").getHost();
        String scheme = ClickHouseChecker.nonEmpty(uri.getScheme(), "Protocol");
        if (template == null) {
            template = DEFAULT;
        }
        if (ClickHouseChecker.isNullOrEmpty(host)) {
            host = template.getHost();
        }

        Map<String, String> params = new LinkedHashMap<>(template.options);
        parseDatabase(uri.getPath(), params);

        ClickHouseUtils.extractParameters(uri.getRawQuery(), params);

        ClickHouseProtocol protocol = ClickHouseProtocol.fromUriScheme(scheme);
        int port = extract(scheme, uri.getPort(), protocol, params);

        ClickHouseCredentials credentials = extract(uri.getRawUserInfo(), params, template.credentials);

        Set<String> tags = new LinkedHashSet<>(template.tags);
        parseTags(uri.getRawFragment(), tags);

        // TODO allow to define scope of client option
        for (String key : new String[] {
                ClickHouseClientOption.LOAD_BALANCING_POLICY.getKey(),
                ClickHouseClientOption.LOAD_BALANCING_TAGS.getKey(),
                ClickHouseClientOption.FAILOVER.getKey(),
                ClickHouseClientOption.NODE_DISCOVERY_INTERVAL.getKey(),
                ClickHouseClientOption.NODE_DISCOVERY_LIMIT.getKey(),
                ClickHouseClientOption.HEALTH_CHECK_INTERVAL.getKey(),
                ClickHouseClientOption.NODE_GROUP_SIZE.getKey(),
                ClickHouseClientOption.CHECK_ALL_NODES.getKey()
        }) {
            if (template.options.containsKey(key)) {
                params.remove(key);
            }
        }

        return new ClickHouseNode(host, protocol, port, credentials, params, tags);
    }

    private final String host;
    private final ClickHouseProtocol protocol;
    private final int port;
    private final ClickHouseCredentials credentials;
    private final Map<String, String> options;
    private final Set<String> tags;
    // cluster-specific properties
    private final String cluster;
    private final int replicaNum;
    private final int shardNum;
    private final int shardWeight;
    private final int weight;
    // cache
    private final String baseUri;
    /**
     * Last update time in milliseconds.
     */
    protected final AtomicLong lastUpdateTime;
    // TODO: metrics

    // consolidated copy of credentials, options and tags
    protected final ClickHouseConfig config;
    protected final AtomicReference<ClickHouseNodeManager> manager;

    protected ClickHouseNode(Builder builder) {
        this(ClickHouseChecker.nonNull(builder, "builder").getHost(), builder.getProtocol(), builder.getPort(),
                builder.getCredentials(), builder.getOptions(), builder.getTags());
    }

    protected ClickHouseNode(String host, ClickHouseProtocol protocol, int port, ClickHouseCredentials credentials,
            Map<String, String> options, Set<String> tags) {
        this.host = ClickHouseChecker.nonEmpty(host, "Host");
        this.protocol = protocol != null ? protocol : ClickHouseProtocol.ANY;
        this.port = port < MIN_PORT_NUM || port > MAX_PORT_NUM ? this.protocol.getDefaultPort() : port;
        this.credentials = credentials;
        if (options != null && !options.isEmpty()) {
            Map<String, String> map = new LinkedHashMap<>(options);
            this.cluster = value(map.remove(PARAM_CLUSTER), "");
            this.replicaNum = value(map.remove(PARAM_REPLICA_NUM), DEFAULT_REPLICA_NUM);
            this.shardNum = value(map.remove(PARAM_SHARD_NUM), DEFAULT_SHARD_NUM);
            this.shardWeight = value(map.remove(PARAM_SHARD_WEIGHT), DEFAULT_SHARD_WEIGHT);
            this.weight = value(map.remove(PARAM_WEIGHT), DEFAULT_WEIGHT);
            this.options = map.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(map);
        } else {
            this.cluster = "";
            this.replicaNum = DEFAULT_REPLICA_NUM;
            this.shardNum = DEFAULT_SHARD_NUM;
            this.shardWeight = DEFAULT_SHARD_WEIGHT;
            this.weight = DEFAULT_WEIGHT;
            this.options = Collections.emptyMap();
        }
        this.lastUpdateTime = new AtomicLong(0L);
        this.tags = tags == null || tags.isEmpty() ? Collections.emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<>(tags));

        this.config = new ClickHouseConfig(ClickHouseConfig.toClientOptions(options), credentials, null, null);
        this.manager = new AtomicReference<>(null);

        StringBuilder builder = new StringBuilder().append(this.protocol.name().toLowerCase(Locale.ROOT));
        if (config.isSsl()) {
            builder.append('s');
        }
        builder.append(SCHEME_DELIMITER);
        builder.append(host).append(':').append(port).append('/');
        this.baseUri = builder.toString();
    }

    protected ClickHouseNode probe() {
        if (protocol != ClickHouseProtocol.ANY) {
            return this;
        }

        ClickHouseNode newNode;
        if (config.isSsl()) {
            newNode = probe(host, port, config.getSslMode(), config.getSslRootCert(), config.getSslCert(),
                    config.getSslKey(), config.getConnectionTimeout());
        } else {
            newNode = probe(host, port, config.getConnectionTimeout());
        }
        return newNode;
    }

    /**
     * Sets manager for this node.
     * 
     * @param m node manager
     * @return this node
     */
    protected ClickHouseNode setManager(ClickHouseNodeManager m) {
        this.manager.getAndUpdate(v -> {
            boolean sameManager = Objects.equals(v, m);
            if (v != null && !sameManager) {
                v.update(ClickHouseNode.this, Status.STANDALONE);
            }
            return sameManager ? v : null;
        });
        if (m != null && manager.compareAndSet(null, m)) {
            m.update(ClickHouseNode.this, Status.MANAGED);
        }
        return this;
    }

    /**
     * Gets socket address to connect to this node.
     *
     * @return socket address to connect to the node
     */
    public InetSocketAddress getAddress() {
        return new InetSocketAddress(host, port);
    }

    /**
     * Gets base URI which is composed of protocol, host and port.
     *
     * @return non-emtpy base URI
     */
    public String getBaseUri() {
        return this.baseUri;
    }

    /**
     * Gets configuration.
     *
     * @return non-null configuration
     */
    public ClickHouseConfig getConfig() {
        return this.config;
    }

    /**
     * Gets credentials for accessing this node. Use
     * {@link ClickHouseConfig#getDefaultCredentials()} if this is not present.
     *
     * @return credentials for accessing this node
     */
    public Optional<ClickHouseCredentials> getCredentials() {
        return Optional.ofNullable(credentials);
    }

    /**
     * Gets credentials for accessing this node. It first attempts to use
     * credentials tied to the node, and then use default credentials from the given
     * configuration.
     * 
     * @param config non-null configuration for retrieving default credentials
     * @return credentials for accessing this node
     */
    public ClickHouseCredentials getCredentials(ClickHouseConfig config) {
        return credentials != null ? credentials
                : ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME).getDefaultCredentials();
    }

    /**
     * Gets host of the node.
     *
     * @return host of the node
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets port of the node.
     *
     * @return port of the node
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets database of the node.
     *
     * @return database of the node
     */
    public Optional<String> getDatabase() {
        return hasPreferredDatabase() ? Optional.of(this.config.getDatabase()) : Optional.empty();
    }

    /**
     * Gets database of the node. When {@link #hasPreferredDatabase()} is
     * {@code false}, it will use database from the given configuration.
     * 
     * @param config non-null configuration to get default database
     * @return database of the node
     */
    public String getDatabase(ClickHouseConfig config) {
        return !ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME).hasOption(ClickHouseClientOption.DATABASE)
                && hasPreferredDatabase() ? this.config.getDatabase() : config.getDatabase();
    }

    /**
     * Gets all options of the node.
     *
     * @return options of the node
     */
    public Map<String, String> getOptions() {
        return this.options;
    }

    /**
     * Gets all tags of the node.
     *
     * @return tags of the node
     */
    public Set<String> getTags() {
        return this.tags;
    }

    /**
     * Gets weight of the node.
     *
     * @return weight of the node
     */
    public int getWeight() {
        return weight;
    }

    /**
     * Gets time zone of the node.
     *
     * @return time zone of the node
     */
    public Optional<TimeZone> getTimeZone() {
        return this.config.hasOption(ClickHouseClientOption.SERVER_TIME_ZONE)
                ? Optional.of(this.config.getServerTimeZone())
                : Optional.empty();
    }

    /**
     * Gets time zone of the node. When not defined, it will use server time zone
     * from the given configuration.
     * 
     * @param config non-null configuration to get server time zone
     * @return time zone of the node
     */
    public TimeZone getTimeZone(ClickHouseConfig config) {
        return this.config.hasOption(ClickHouseClientOption.SERVER_TIME_ZONE) ? this.config.getServerTimeZone()
                : ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME).getServerTimeZone();
    }

    /**
     * Gets version of the node.
     *
     * @return version of the node
     */
    public Optional<ClickHouseVersion> getVersion() {
        return this.config.hasOption(ClickHouseClientOption.SERVER_VERSION)
                ? Optional.of(this.config.getServerVersion())
                : Optional.empty();
    }

    /**
     * Gets version of the node. When not defined, it will use server version from
     * the given configuration.
     * 
     * @param config non-null configuration to get server version
     * @return version of the node
     */
    public ClickHouseVersion getVersion(ClickHouseConfig config) {
        return this.config.hasOption(ClickHouseClientOption.SERVER_VERSION) ? this.config.getServerVersion()
                : ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME).getServerVersion();
    }

    /**
     * Gets cluster name of the node.
     *
     * @return cluster name of node
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * Gets protocol used by the node.
     *
     * @return protocol used by the node
     */
    public ClickHouseProtocol getProtocol() {
        return this.protocol;
    }

    /**
     * Checks if preferred database was specified or not. When preferred database
     * was not specified, {@link #getDatabase()} will return default database.
     *
     * @return true if preferred database was specified; false otherwise
     */
    public boolean hasPreferredDatabase() {
        return this.config.hasOption(ClickHouseClientOption.DATABASE);
    }

    /**
     * Checks whether the node is managed by a {@link ClickHouseCluster} ro
     * {@link ClickHouseNodes}.
     *
     * @return true if the node is managed; false otherwise
     */
    public boolean isManaged() {
        return manager.get() != null;
    }

    /**
     * Checks whether the node is not managed by any {@link ClickHouseCluster} ro
     * {@link ClickHouseNodes}.
     *
     * @return true if the node is not managed; false otherwise
     */
    public boolean isStandalone() {
        return manager.get() == null;
    }

    /**
     * Checks if the given node has same base URI as current one.
     *
     * @param node node to test
     * @return true if the nodes are same endpoint; false otherwise
     */
    public boolean isSameEndpoint(ClickHouseNode node) {
        if (node == null) {
            return false;
        }

        return baseUri.equals(node.baseUri);
    }

    /**
     * Updates status of the node. This will only work when the node is
     * managed({@link #isManaged()} returns {@code true}).
     * 
     * @param status non-null node status
     */
    public void update(Status status) {
        ClickHouseNodeManager m = this.manager.get();
        if (m != null) {
            m.update(this, status);
        }
    }

    @Override
    public ClickHouseNode apply(ClickHouseNodeSelector t) {
        final ClickHouseNodeManager m = manager.get();
        if (m != null) { // managed
            return m.apply(m.getNodeSelector());
        }

        if (t != null && t != ClickHouseNodeSelector.EMPTY
                && (!t.matchAnyOfPreferredProtocols(protocol) || !t.matchAllPreferredTags(tags))) {
            throw new IllegalArgumentException(ClickHouseUtils.format("%s expects a node rather than %s", t, this));
        }
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, protocol, credentials, options, tags, cluster, replicaNum, shardNum,
                shardWeight, weight);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseNode other = (ClickHouseNode) obj;
        return host.equals(other.host) && port == other.port && protocol == other.protocol
                && Objects.equals(credentials, other.credentials) && options.equals(other.options)
                && tags.equals(other.tags) && cluster.equals(other.cluster) && replicaNum == other.replicaNum
                && shardNum == other.shardNum && shardWeight == other.shardWeight && weight == other.weight;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder().append("ClickHouseNode [uri=").append(baseUri)
                .append(config.getDatabase());
        if (!cluster.isEmpty()) {
            builder.append(", cluster=").append(cluster).append("(s").append(shardNum).append(",w").append(shardWeight)
                    .append(",r").append(replicaNum).append(')');
        }

        Map<String, ClickHouseOption> m = ClickHouseConfig.ClientOptions.INSTANCE.sensitiveOptions;
        StringBuilder optsBuilder = new StringBuilder();
        for (Entry<String, String> option : options.entrySet()) {
            String key = option.getKey();
            if (!ClickHouseClientOption.DATABASE.getKey().equals(key)
                    && !ClickHouseClientOption.SSL.getKey().equals(key)) {
                optsBuilder.append(key).append('=').append(m.containsKey(key) ? "*" : option.getValue()).append(',');
            }
        }
        if (optsBuilder.length() > 0) {
            optsBuilder.setLength(optsBuilder.length() - 1);
            builder.append(", options={").append(optsBuilder).append('}');
        }
        if (!tags.isEmpty()) {
            builder.append(", tags=").append(tags);
        }
        return builder.append("]@").append(hashCode()).toString();
    }

    /**
     * Converts to URI, without credentials for security reason.
     *
     * @return non-null URI
     */
    public URI toUri() {
        return toUri(null);
    }

    /**
     * Converts to URI, without credentials for security reason.
     *
     * @param schemePrefix optional prefix of scheme
     * @return non-null URI
     */
    public URI toUri(String schemePrefix) {
        StringBuilder builder = new StringBuilder();
        String db = getDatabase().orElse(null);
        for (Entry<String, String> entry : options.entrySet()) {
            if (ClickHouseClientOption.DATABASE.getKey().equals(entry.getKey())) {
                db = entry.getValue();
                continue;
            }
            builder.append(entry.getKey()).append('=').append(entry.getValue()).append('&');
        }
        String query = null;
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
            query = builder.toString();
        }

        builder.setLength(0);
        boolean first = true;
        for (String tag : tags) {
            if (first) {
                first = false;
            } else {
                builder.append(',');
            }
            builder.append(ClickHouseUtils.encode(tag));
        }

        String p = protocol.name().toLowerCase(Locale.ROOT);
        if (ClickHouseChecker.isNullOrEmpty(schemePrefix)) {
            schemePrefix = p;
        } else {
            if (schemePrefix.charAt(schemePrefix.length() - 1) == ':') {
                schemePrefix = schemePrefix.concat(p);
            } else {
                schemePrefix = new StringBuilder(schemePrefix).append(':').append(p).toString();
            }
        }
        try {
            return new URI(schemePrefix, null, host, port, ClickHouseChecker.isNullOrEmpty(db) ? "" : "/".concat(db),
                    query, builder.length() > 0 ? builder.toString() : null);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }
}
