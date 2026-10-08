package com.clickhouse.client;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.util.Collections;
import java.util.List;

/**
 * One ClickHouse server, published on the Docker host.
 */
final class SingleClickHouseContainerStarter extends ClickHouseContainerStarter {
    private final GenericContainer<?> container;

    SingleClickHouseContainerStarter(ClickHouseTestEnvironment environment, Network network) {
        super(environment, network);
        this.container = newClickHouseServer(FRONTEND_ALIAS, environment.getContainerName());
    }

    @Override
    public void start() {
        container.start();
    }

    @Override
    public void stop() {
        stopQuietly(container);
    }

    @Override
    public boolean isRunning() {
        return container.isRunning();
    }

    @Override
    public String getHost() {
        return container.getHost();
    }

    @Override
    public int getMappedPort(int port) {
        return container.getMappedPort(port);
    }

    @Override
    public Container.ExecResult execInContainer(String... command) throws Exception {
        return container.execInContainer(command);
    }

    @Override
    public GenericContainer<?> getEndpointContainer() {
        return container;
    }

    @Override
    protected List<GenericContainer<?>> clickHouseServers() {
        return Collections.singletonList(container);
    }
}
