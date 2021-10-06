package com.clickhouse.client;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import com.clickhouse.client.config.ClickHouseDefaults;

/**
 * This class depicts a ClickHouse server, essentially a combination of host,
 * port and protocol, for client to connect.
 */
public class ClickHouseNode implements Function<ClickHouseNodeSelector, ClickHouseNode>, Serializable {
    /**
     * Node status.
     */
    public enum Status {
        HEALTHY, UNHEALTHY, MANAGED, UNMANAGED
    }

    /**
     * Mutable and non-thread safe builder.
     */
    public static class Builder {
        protected String cluster;
        protected String host;
        protected Integer port;
        protected InetSocketAddress address;
        protected ClickHouseProtocol protocol;
        protected ClickHouseCredentials credentials;
        protected String database;
        // label is more expressive, but is slow for comparison
        protected Set<String> tags;
        protected Integer weight;

        /**
         * Default constructor.
         */
        protected Builder() {
            this.tags = new HashSet<>(3);
        }

        protected String getCluster() {
            if (cluster == null) {
                cluster = (String) ClickHouseDefaults.CLUSTER.getEffectiveDefaultValue();
            }

            return cluster;
        }

        protected InetSocketAddress getAddress() {
            if (host == null) {
                host = (String) ClickHouseDefaults.HOST.getEffectiveDefaultValue();
            }

            if (port == null) {
                port = (Integer) ClickHouseDefaults.PORT.getEffectiveDefaultValue();
            }

            if (address == null) {
                address = InetSocketAddress.createUnresolved(host, port);
            }

            return address;
        }

        protected ClickHouseProtocol getProtocol() {
            if (protocol == null) {
                protocol = ClickHouseProtocol.valueOf((String) ClickHouseDefaults.PROTOCOL.getEffectiveDefaultValue());
            }
            return protocol;
        }

        protected ClickHouseCredentials getCredentials() {
            return credentials;
        }

        protected String getDatabase() {
            if (database == null) {
                database = (String) ClickHouseDefaults.DATABASE.getEffectiveDefaultValue();
            }
            return database;
        }

        protected Set<String> getTags() {
            int size = tags == null ? 0 : tags.size();
            if (size == 0) {
                return Collections.emptySet();
            }

            Set<String> s = new HashSet<>(size);
            s.addAll(tags);
            return Collections.unmodifiableSet(s);
        }

        protected int getWeight() {
            if (weight == null) {
                weight = (Integer) ClickHouseDefaults.WEIGHT.getEffectiveDefaultValue();
            }

            return weight.intValue();
        }

        /**
         * Sets cluster name.
         *
         * @param cluster cluster name, null means {@link ClickHouseDefaults#CLUSTER}
         * @return this builder
         */
        public Builder cluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        /**
         * Sets host name.
         *
         * @param host host name, null means {@link ClickHouseDefaults#HOST}
         * @return this builder
         */
        public Builder host(String host) {
            if (!Objects.equals(this.host, host)) {
                this.address = null;
            }

            this.host = host;
            return this;
        }

        /**
         * Sets port number.
         *
         * @param port port number, null means {@link ClickHouseDefaults#PORT}
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
            return port(protocol, ClickHouseChecker.nonNull(protocol, "protocol").getDefaultPort());
        }

        /**
         * Sets protocol and port number.
         *
         * @param protocol protocol, null means {@link ClickHouseDefaults#PROTOCOL}
         * @param port     number, null means {@link ClickHouseDefaults#PORT}
         * @return this builder
         */
        public Builder port(ClickHouseProtocol protocol, Integer port) {
            if (!Objects.equals(this.port, port)) {
                this.address = null;
            }

            this.port = port;
            this.protocol = protocol;
            return this;
        }

        /**
         * Sets socket address.
         *
         * @param address socket address, null means {@link ClickHouseDefaults#HOST} and
         *                {@link ClickHouseDefaults#PORT}
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
         *                 and {@link ClickHouseDefaults#PORT}
         * @return this builder
         */
        public Builder address(ClickHouseProtocol protocol, InetSocketAddress address) {
            if (!Objects.equals(this.address, address)) {
                this.host = null;
                this.port = null;
            }

            this.address = address;
            this.protocol = protocol;
            return this;
        }

        /**
         * Sets database name.
         *
         * @param database database name, null means {@link ClickHouseDefaults#DATABASE}
         * @return this builder
         */
        public Builder database(String database) {
            this.database = database;
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
         * Adds a tag for this node.
         *
         * @param tag tag for the node, null or duplicate tag will be ignored
         * @return this builder
         */
        public Builder addTag(String tag) {
            if (tag != null) {
                this.tags.add(tag);
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
            if (tag != null) {
                this.tags.remove(tag);
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
                    if (t == null) {
                        continue;
                    }
                    this.tags.add(t);
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
                    if (t == null) {
                        continue;
                    }
                    this.tags.add(t);
                }
            }

            return this;
        }

        /**
         * Sets weight of this node.
         *
         * @param weight weight of the node, null means
         *               {@link ClickHouseDefaults#WEIGHT}
         * @return this builder
         */
        public Builder weight(Integer weight) {
            this.weight = weight;
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
            b.cluster(base.getCluster()).host(base.getHost()).port(base.getProtocol(), base.getPort())
                    .credentials(base.getCredentials().orElse(null)).database(base.getDatabase()).tags(base.getTags())
                    .weight(base.getWeight());
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
     * @return node object
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
     * Generated UID.
     */
    private static final long serialVersionUID = 8342604784121795372L;

    private final String cluster;
    private final ClickHouseProtocol protocol;
    private final InetSocketAddress address;
    private final Optional<ClickHouseCredentials> credentials;
    private final String database;
    private final Set<String> tags;
    private final int weight;

    private BiConsumer<ClickHouseNode, Status> manager;

    protected ClickHouseNode(Builder builder) {
        ClickHouseChecker.nonNull(builder, "builder");

        this.cluster = builder.getCluster();
        this.protocol = builder.getProtocol();
        this.address = builder.getAddress();
        this.credentials = Optional.ofNullable(builder.getCredentials());
        this.database = builder.getDatabase();
        this.tags = builder.getTags();
        this.weight = builder.getWeight();

        this.manager = null;
    }

    /**
     * Gets socket address to connect to this node.
     *
     * @return socket address to connect to the node
     */
    public InetSocketAddress getAddress() {
        return this.address;
    }

    /**
     * Gets credentials for accessing this node. Use
     * {@link ClickHouseConfig#getDefaultCredentials()} if this is not present.
     *
     * @return credentials for accessing this node
     */
    public Optional<ClickHouseCredentials> getCredentials() {
        return this.credentials;
    }

    /**
     * Gets host of the node.
     *
     * @return host of the node
     */
    public String getHost() {
        return this.address.getHostString();
    }

    /**
     * Gets port of the node.
     *
     * @return port of the node
     */
    public int getPort() {
        return this.address.getPort();
    }

    /**
     * Gets database of the node.
     *
     * @return database of the node
     */
    public String getDatabase() {
        return this.database;
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
        return this.weight;
    }

    /**
     * Gets cluster name of the node.
     *
     * @return cluster name of node
     */
    public String getCluster() {
        return this.cluster;
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
     * Sets manager for this node.
     * 
     * @param manager function to manage status of the node
     */
    public synchronized void setManager(BiConsumer<ClickHouseNode, Status> manager) {
        if (this.manager != null && !this.manager.equals(manager)) {
            this.manager.accept(this, Status.UNMANAGED);
        }

        if (manager != null && !manager.equals(this.manager)) {
            manager.accept(this, Status.MANAGED);
            this.manager = manager;
        }
    }

    /**
     * Updates status of the node. This will only work when a manager function
     * exists via {@link #setManager(BiConsumer)}.
     * 
     * @param status node status
     */
    public synchronized void updateStatus(Status status) {
        if (this.manager != null) {
            this.manager.accept(this, status);
        }
    }

    @Override
    public ClickHouseNode apply(ClickHouseNodeSelector t) {
        if (t != null && t != ClickHouseNodeSelector.EMPTY
                && (!t.matchAnyOfPreferredProtocols(protocol) || !t.matchAllPreferredTags(tags))) {
            throw new IllegalArgumentException("No suitable node found");
        }

        return this;
    }

    @Override
    public String toString() {
        return new StringBuilder().append(getClass().getSimpleName()).append('(').append("cluster=").append(cluster)
                .append(", protocol=").append(protocol).append(", address=").append(address).append(", database=")
                .append(database).append(", tags=").append(tags).append(", weight=").append(weight).append(')')
                .append('@').append(hashCode()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, cluster, credentials, database, protocol, tags, weight);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        if (this == obj) {
            return true;
        }

        ClickHouseNode node = (ClickHouseNode) obj;
        return address.equals(node.address) && cluster.equals(node.cluster) && credentials.equals(node.credentials)
                && database.equals(node.database) && protocol.equals(node.protocol) && tags.equals(node.tags)
                && weight == node.weight;
    }
}
