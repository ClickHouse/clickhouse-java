package com.clickhouse.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;

@Deprecated
public class ClickHouseCluster extends ClickHouseNodes {
    private static final long serialVersionUID = 8684489015067906319L;

    /**
     * Creates cluster object from list of nodes.
     *
     * @param node first node
     * @param more more nodes if any
     * @return cluster object
     */
    public static ClickHouseCluster of(ClickHouseNode node, ClickHouseNode... more) {
        return of(null, node, more);
    }

    /**
     * Creates cluster object from list of nodes.
     *
     * @param cluster cluster name
     * @param node    first node
     * @param more    more nodes if any
     * @return cluster object
     */
    public static ClickHouseCluster of(String cluster, ClickHouseNode node, ClickHouseNode... more) {
        if (node == null) {
            throw new IllegalArgumentException("At least one non-null node is required");
        }

        List<ClickHouseNode> list = new LinkedList<>();
        list.add(node);
        if (more != null) {
            list.addAll(Arrays.asList(more));
        }
        return of(cluster, list);
    }

    public static ClickHouseCluster of(String cluster, Collection<ClickHouseNode> nodes) {
        if (nodes == null || nodes.isEmpty() || nodes.iterator().next() == null) {
            throw new IllegalArgumentException("At least one non-null node is required");
        }

        boolean autoDiscovery = false;
        String clusterName = cluster;
        int size = nodes.size();
        List<ClickHouseNode> list = new ArrayList<>(size);
        for (ClickHouseNode n : nodes) {
            if (n == null) {
                continue;
            }
            autoDiscovery = autoDiscovery || n.config.getBoolOption(ClickHouseClientOption.AUTO_DISCOVERY);
            String name = n.getCluster();
            if (!ClickHouseChecker.isNullOrEmpty(name)) {
                if (ClickHouseChecker.isNullOrEmpty(clusterName)) {
                    clusterName = name;
                } else if (!name.equals(clusterName)) {
                    throw new IllegalArgumentException(
                            ClickHouseUtils.format(
                                    "Cluster name should be [%s] for all %d node(s), but it's [%s] for %s", clusterName,
                                    size, name, n));
                }
            }
        }
        if (autoDiscovery && ClickHouseChecker.isNullOrEmpty(clusterName)) {
            throw new IllegalArgumentException("Please specify non-empty cluster name in order to use auto discovery");
        }
        for (ClickHouseNode n : nodes) {
            if (n == null) {
                continue;
            }

            n = clusterName.equals(n.getCluster()) ? n : ClickHouseNode.builder(n).cluster(clusterName).build();
            if (!list.contains(n)) {
                list.add(n);
            }
        }
        return new ClickHouseCluster(clusterName, list);
    }

    private final String clusterName;

    /**
     * Constructs cluster object using policy and list of nodes. It could be slow
     * when {@link ClickHouseClientOption#AUTO_DISCOVERY} is enabled.
     *
     * @param cluster non-null cluster name
     * @param nodes   list of nodes
     */
    protected ClickHouseCluster(String cluster, Collection<ClickHouseNode> nodes) {
        super(nodes);
        this.clusterName = cluster;
    }

    public String getCluster() {
        return clusterName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + clusterName.hashCode();
        result = prime * result + checking.hashCode();
        result = prime * result + index.hashCode();
        result = prime * result + policy.hashCode();
        result = prime * result + nodes.hashCode();
        result = prime * result + faultyNodes.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseCluster other = (ClickHouseCluster) obj;
        // FIXME ignore order when comparing node lists
        return clusterName.equals(other.clusterName) && policy.equals(other.policy) && nodes.equals(other.nodes)
                && faultyNodes.equals(other.faultyNodes);
    }

    @Override
    public String toString() {
        return new StringBuilder("ClickHouseCluster [name=").append(clusterName).append(", checking=")
                .append(checking.get()).append(", index=").append(index.get()).append(", lock=r")
                .append(lock.getReadHoldCount()).append('w').append(lock.getWriteHoldCount()).append(", nodes=")
                .append(nodes.size()).append(", faulty=").append(faultyNodes.size()).append(", policy=")
                .append(policy.getClass().getSimpleName()).append("]@").append(hashCode()).toString();
    }
}
