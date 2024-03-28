package com.clickhouse.client.grpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.UnsupportedProtocolException;
import com.clickhouse.client.grpc.config.ClickHouseGrpcOption;
import com.clickhouse.config.ClickHouseOption;
@Deprecated
public class ClickHouseGrpcClient implements ClickHouseClient {
    private final AtomicReference<ClickHouseClient> ref;

    protected ClickHouseClient getInstance() {
        ClickHouseClient instance = ref.get();
        if (instance == null) {
            try {
                instance = new ClickHouseGrpcClientImpl();
            } catch (ExceptionInInitializerError | NoClassDefFoundError e) {
                throw new UnsupportedProtocolException(ClickHouseProtocol.GRPC,
                        "gRPC is not supported. Please use http protocol or add gRPC libraries to the classpath.");
            }
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
    public final boolean accept(ClickHouseProtocol protocol) {
        return ClickHouseProtocol.GRPC == protocol || ClickHouseClient.super.accept(protocol);
    }

    @Override
    public final Class<? extends ClickHouseOption> getOptionClass() {
        return ClickHouseGrpcOption.class;
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