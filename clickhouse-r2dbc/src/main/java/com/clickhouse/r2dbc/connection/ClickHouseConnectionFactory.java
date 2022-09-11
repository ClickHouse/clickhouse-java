package com.clickhouse.r2dbc.connection;


import com.clickhouse.client.ClickHouseNodes;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;

public class ClickHouseConnectionFactory implements ConnectionFactory {


    private final ClickHouseNodes nodes;

    ClickHouseConnectionFactory(ClickHouseNodes nodes) {
        this.nodes = nodes;
    }

    @Override
    public Mono<? extends Connection> create() {
        return Mono.just(new ClickHouseConnection(nodes));
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return ClickHouseConnectionFactoryMetadata.INSTANCE;
    }
}
