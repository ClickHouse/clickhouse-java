package com.clickhouse.client.grpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.config.ClickHouseOption;

public class ClickHouseGrpcClient implements ClickHouseClient {
    private final AtomicReference<ClickHouseClient> ref;

    protected ClickHouseClient getInstance() {
        ClickHouseClient instance = ref.get();
        if (instance == null) {
            instance = new ClickHouseGrpcClientImpl();
            if (!ref.compareAndSet(null, instance)) {
                instance.close();
                instance = ref.get();
            }
        }
        return instance;
    }

    public ClickHouseGrpcClient() {
        ref = new AtomicReference<>();
    }

    @Override
    public boolean accept(ClickHouseProtocol protocol) {
        return getInstance().accept(protocol);
    }

    @Override
    public Class<? extends ClickHouseOption> getOptionClass() {
        return getInstance().getOptionClass();
    }

    @Override
    public void init(ClickHouseConfig config) {
        getInstance().init(config);
    }

    @Override
    public boolean ping(ClickHouseNode server, int timeout) {
        return getInstance().ping(server, timeout);
    }

    @Override
    public CompletableFuture<ClickHouseResponse> execute(ClickHouseRequest<?> request) {
        return getInstance().execute(request);
    }

    @Override
    public ClickHouseConfig getConfig() {
        return getInstance().getConfig();
    }

    @Override
    public void close() {
        getInstance().close();
    }
}