package com.clickhouse.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * This class maintains two immutable collections: preferred protocols and tags.
 * Usually it will be used in two scenarios: 1) find suitable
 * {@link ClickHouseClient} according to preferred protocol(s); and 2) pick
 * suitable {@link ClickHouseNode} to connect to.
 */
@Deprecated
public class ClickHouseNodeSelector implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseNodeSelector.class);
    private static final long serialVersionUID = 488571984297086418L;

    /**
     * Empty node selector matches all clients and nodes.
     */
    public static final ClickHouseNodeSelector EMPTY = new ClickHouseNodeSelector(Collections.emptyList(),
            Collections.emptyList());

    /**
     * Creates a node selector by specifying preferred protocols.
     *
     * @param protocol preferred protocol
     * @param more     more protocols
     * @return non-null node selector
     */
    public static ClickHouseNodeSelector of(ClickHouseProtocol protocol, ClickHouseProtocol... more) {
        List<ClickHouseProtocol> list = new LinkedList<>();

        if (protocol != null) {
            list.add(protocol);
        }

        if (more != null) {
            for (ClickHouseProtocol p : more) {
                if (p != null) {
                    list.add(p);
                }
            }
        }

        return of(list, null);
    }

    /**
     * Creates a node selector by specifying preferred tags.
     *
     * @param tag  preferred tag
     * @param more more tags
     * @return non-null selector
     */
    public static ClickHouseNodeSelector of(String tag, String... more) {
        List<String> list = new LinkedList<>();

        if (!ClickHouseChecker.isNullOrEmpty(tag)) {
            list.add(tag);
        }

        if (more != null) {
            for (String t : more) {
                if (!ClickHouseChecker.isNullOrEmpty(t)) {
                    list.add(t);
                }
            }
        }

        return of(null, list);
    }

    /**
     * Creates a node selector by specifying preferred protocols and tags.
     *
     * @param protocols preferred protocols
     * @param tags      preferred tags
     * @return non-null selector
     */
    public static ClickHouseNodeSelector of(Collection<ClickHouseProtocol> protocols, Collection<String> tags) {
        return (protocols == null || protocols.isEmpty()) && (tags == null || tags.isEmpty()) ? EMPTY
                : new ClickHouseNodeSelector(protocols, tags);
    }

    private final List<ClickHouseProtocol> protocols;
    private final Set<String> tags;

    /**
     * Default constructor.
     *
     * @param protocols preferred protocols
     * @param tags      preferred tags
     */
    protected ClickHouseNodeSelector(Collection<ClickHouseProtocol> protocols, Collection<String> tags) {
        if (protocols == null || protocols.isEmpty()) {
            this.protocols = Collections.emptyList();
        } else {
            List<ClickHouseProtocol> p = new ArrayList<>(protocols.size());
            for (ClickHouseProtocol protocol : protocols) {
                if (protocol == ClickHouseProtocol.ANY) {
                    p.clear();
                    break;
                } else if (protocol != null && !p.contains(protocol)) {
                    p.add(protocol);
                }
            }
            this.protocols = p.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(p);
        }

        if (tags == null || tags.isEmpty()) {
            this.tags = Collections.emptySet();
        } else {
            Set<String> t = new HashSet<>();
            for (String tag : tags) {
                if (tag != null && !(tag = tag.trim()).isEmpty()) {
                    t.add(tag);
                }
            }
            this.tags = t.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(t);
        }
    }

    /**
     * Gets preferred protocols. Empty list means all.
     *
     * @return non-null preferred protocols
     */
    public List<ClickHouseProtocol> getPreferredProtocols() {
        return this.protocols;
    }

    /**
     * Gets preferred tags. Empty set means all.
     *
     * @return non-null preferred tags
     */
    public Set<String> getPreferredTags() {
        return this.tags;
    }

    /**
     * Test if the given client supports any of {@link #getPreferredProtocols()}.
     * It's always {@code false} if either the client is null or there's no
     * preferred protocol.
     *
     * @param client client to test
     * @return true if any of the preferred protocols is supported by the client
     */
    public boolean match(ClickHouseClient client) {
        boolean matched = false;

        if (client != null) {
            for (ClickHouseProtocol p : protocols) {
                log.debug("Checking [%s] against [%s]...", client, p);
                if (client.accept(p)) {
                    matched = true;
                    break;
                }
            }
        }

        return matched;
    }

    /**
     * Checks if the given node matches any of preferred protocols and tags.
     *
     * @param node node to check
     * @return true if the node matches at least one of preferred protocols and
     *         tags; false otherwise
     */
    public boolean match(ClickHouseNode node) {
        boolean matched = false;

        if (node != null) {
            matched = matchAnyOfPreferredProtocols(node.getProtocol()) && matchAllPreferredTags(node.getTags());
        }

        return matched;
    }

    /**
     * Checks if the given protocol matches any of the preferred protocols.
     *
     * @param protocol protocol to check
     * @return true if the protocol matches at least one of preferred protocols;
     *         false otherwise
     */
    public boolean matchAnyOfPreferredProtocols(ClickHouseProtocol protocol) {
        boolean matched = protocols.isEmpty() || protocol == ClickHouseProtocol.ANY;

        if (!matched && protocol != null) {
            for (ClickHouseProtocol p : protocols) {
                if (p == protocol) {
                    matched = true;
                    break;
                }
            }
        }

        return matched;
    }

    /**
     * Checks if the preferred tags contain all of given tags.
     *
     * @param tags tags to check
     * @return true if the preferred tags contain all of given tags; false otherwise
     */
    public boolean matchAllPreferredTags(Collection<String> tags) {
        if (this.tags.isEmpty() || tags == null) {
            return tags == null || tags.isEmpty();
        }

        return this.tags.containsAll(tags);
    }

    /**
     * Checks if the preferred tags contain any of given tags.
     *
     * @param tags tags to check
     * @return true if the preferred tags contain any of given tags; false otherwise
     */
    public boolean matchAnyOfPreferredTags(Collection<String> tags) {
        boolean matched = tags == null || tags.isEmpty();

        if (!matched) {
            for (String t : tags) {
                if (this.tags.contains(t)) {
                    matched = true;
                    break;
                }
            }
        }

        return matched;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + protocols.hashCode();
        result = prime * result + tags.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseNodeSelector other = (ClickHouseNodeSelector) obj;
        return protocols.equals(other.protocols) && tags.equals(other.tags);
    }

    @Override
    public String toString() {
        return new StringBuilder().append("ClickHouseNodeSelector [protocols=").append(protocols).append(", tags=")
                .append(tags).append(']').toString();
    }
}
