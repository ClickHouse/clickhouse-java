package com.clickhouse.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * This class maintains two immutable lists: preferred protocols and tags.
 * Usually it will be used in two scenarios: 1) find suitable
 * {@link ClickHouseClient} according to preferred protocol(s); and 2) pick
 * suitable {@link ClickHouseNode} to connect to.
 */
public class ClickHouseNodeSelector implements Serializable {
    public static final ClickHouseNodeSelector EMPTY = new ClickHouseNodeSelector(Collections.emptyList(),
            Collections.emptyList());

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

    public static ClickHouseNodeSelector of(Collection<ClickHouseProtocol> protocols, Collection<String> tags) {
        return (protocols == null || protocols.size() == 0) && (tags == null || tags.size() == 0) ? EMPTY
                : new ClickHouseNodeSelector(protocols, tags);
    }

    private static final long serialVersionUID = 488571984297086418L;

    private final List<ClickHouseProtocol> protocols;
    private final Set<String> tags;

    protected ClickHouseNodeSelector(Collection<ClickHouseProtocol> protocols, Collection<String> tags) {
        if (protocols == null || protocols.size() == 0) {
            this.protocols = Collections.emptyList();
        } else {
            List<ClickHouseProtocol> p = new ArrayList<>(protocols.size());
            for (ClickHouseProtocol protocol : protocols) {
                if (protocol == null) {
                    continue;
                } else if (protocol == ClickHouseProtocol.ANY) {
                    p.clear();
                    break;
                } else if (!p.contains(protocol)) {
                    p.add(protocol);
                }
            }

            this.protocols = p.size() == 0 ? Collections.emptyList() : Collections.unmodifiableList(p);
        }

        if (tags == null || tags.size() == 0) {
            this.tags = Collections.emptySet();
        } else {
            Set<String> t = new HashSet<>();
            for (String tag : tags) {
                if (tag == null || tag.isEmpty()) {
                    continue;
                } else {
                    t.add(tag);
                }
            }

            this.tags = t.size() == 0 ? Collections.emptySet() : Collections.unmodifiableSet(t);
        }
    }

    public List<ClickHouseProtocol> getPreferredProtocols() {
        return this.protocols;
    }

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
                if (client.accept(p)) {
                    matched = true;
                    break;
                }
            }
        }

        return matched;
    }

    public boolean match(ClickHouseNode node) {
        boolean matched = false;

        if (node != null) {
            matched = matchAnyOfPreferredProtocols(node.getProtocol()) && matchAllPreferredTags(node.getTags());
        }

        return matched;
    }

    public boolean matchAnyOfPreferredProtocols(ClickHouseProtocol protocol) {
        boolean matched = protocols.size() == 0 || protocol == ClickHouseProtocol.ANY;

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

    public boolean matchAllPreferredTags(Collection<String> tags) {
        boolean matched = true;

        if (tags != null && tags.size() > 0) {
            for (String t : tags) {
                if (t == null || t.isEmpty()) {
                    continue;
                }

                matched = matched && this.tags.contains(t);

                if (!matched) {
                    break;
                }
            }
        }

        return matched;
    }

    public boolean matchAnyOfPreferredTags(Collection<String> tags) {
        boolean matched = tags.size() == 0;

        if (tags != null && tags.size() > 0) {
            for (String t : tags) {
                if (t == null || t.isEmpty()) {
                    continue;
                }

                if (this.tags.contains(t)) {
                    matched = true;
                    break;
                }
            }
        }

        return matched;
    }

    /*
     * public boolean matchAnyOfPreferredTags(String cluster,
     * List<ClickHouseProtocol> protocols, List<String> tags) { return
     * (ClickHouseChecker.isNullOrEmpty(cluster) || cluster.equals(this.cluster)) &&
     * supportAnyProtocol(protocols) && hasAllTags(tags); }
     */
}
