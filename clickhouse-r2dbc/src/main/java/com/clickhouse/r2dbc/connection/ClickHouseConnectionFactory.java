package com.clickhouse.r2dbc.connection;

import java.util.function.Function;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;

public class ClickHouseConnectionFactory implements ConnectionFactory {
    private final Function<ClickHouseNodeSelector, ClickHouseNode> nodes;

    ClickHouseConnectionFactory(Function<ClickHouseNodeSelector, ClickHouseNode> nodes) {
        this.nodes = nodes;
    }

    @Override
    public Mono<? extends Connection> create() {
        return Mono.defer(() -> Mono.just(new ClickHouseConnection(nodes)));
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return ClickHouseConnectionFactoryMetadata.INSTANCE;
    }
}
