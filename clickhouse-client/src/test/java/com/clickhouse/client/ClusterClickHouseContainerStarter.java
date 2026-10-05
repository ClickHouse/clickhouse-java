package com.clickhouse.client;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Three ClickHouse replicas of one shard, with a shared Keeper and nginx in front.
 */
final class ClusterClickHouseContainerStarter extends ClickHouseContainerStarter {
    static final String CLUSTER_NAME = "test_cluster";
    static final int NODE_COUNT = 3;
    private static final String NGINX_CONFIG = "containers/nginx/nginx.conf";
    private static final String KEEPER_CONFIG = "containers/clickhouse-keeper/keeper_config.xml";
    private static final int KEEPER_PORT = 9181;

    private final GenericContainer<?> keeper;
    private final List<GenericContainer<?>> nodes;
    private final GenericContainer<?> nginx;

    ClusterClickHouseContainerStarter(ClickHouseTestEnvironment environment, Network network) {
        super(environment, network);
        String baseName = environment.getContainerName();
        this.keeper = new GenericContainer<>(environment.getImageRef())
                .withCreateContainerCmdModifier(command -> {
                    if (baseName != null) {
                        command.withName(baseName + "-keeper");
                    }
                })
                .withNetwork(network)
                .withNetworkAliases("clickhouse-keeper")
                .withCommand("clickhouse-keeper", "--config-file=/etc/clickhouse-keeper/keeper_config.xml")
                .withClasspathResourceMapping(KEEPER_CONFIG, "/etc/clickhouse-keeper/keeper_config.xml",
                        BindMode.READ_ONLY)
                .withExposedPorts(KEEPER_PORT)
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.of(120, SECONDS)));

        List<GenericContainer<?>> created = new ArrayList<GenericContainer<?>>(NODE_COUNT);
        for (int index = 1; index <= NODE_COUNT; index++) {
            String name = baseName == null ? null : baseName + "-" + index;
            created.add(newClickHouseServer("clickhouse-" + index, name, "r" + index));
        }
        this.nodes = created;

        String nginxName = baseName == null ? null : baseName + "-nginx";
        this.nginx = newNginx(nginxName, NGINX_CONFIG);
    }

    @Override
    public String replicatedClusterName() {
        return CLUSTER_NAME;
    }

    @Override
    public void start() {
        List<GenericContainer<?>> started = new ArrayList<GenericContainer<?>>();
        try {
            keeper.start();
            for (GenericContainer<?> node : nodes) {
                node.start();
                started.add(node);
            }
            nginx.start();
        } catch (RuntimeException failure) {
            stopQuietly(nginx);
            for (int index = started.size() - 1; index >= 0; index--) {
                stopQuietly(started.get(index));
            }
            stopQuietly(keeper);
            throw failure;
        }
    }

    @Override
    public void stop() {
        stopQuietly(nginx);
        for (int index = nodes.size() - 1; index >= 0; index--) {
            stopQuietly(nodes.get(index));
        }
        stopQuietly(keeper);
    }

    @Override
    public boolean isRunning() {
        if (!keeper.isRunning() || !nginx.isRunning()) {
            return false;
        }
        for (GenericContainer<?> node : nodes) {
            if (!node.isRunning()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String getHost() {
        return nginx.getHost();
    }

    @Override
    public int getMappedPort(int port) {
        return nginx.getMappedPort(port);
    }

    @Override
    public Container.ExecResult execInContainer(String... command) throws Exception {
        return nodes.get(0).execInContainer(command);
    }

    @Override
    public GenericContainer<?> getEndpointContainer() {
        return nginx;
    }

    @Override
    protected List<GenericContainer<?>> clickHouseServers() {
        return nodes;
    }
}
