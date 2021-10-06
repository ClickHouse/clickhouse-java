package com.clickhouse.client.grpc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.exception.ClickHouseException;
import com.clickhouse.client.grpc.impl.QueryInfo;

@Deprecated
public class ClickHouseGrpcFuture implements Future<ClickHouseResponse> {
    private final ClickHouseNode server;
    private final ClickHouseRequest<?> request;

    private final StreamObserver<QueryInfo> requestObserver;
    private final ClickHouseStreamObserver responseObserver;

    protected ClickHouseGrpcFuture(ClickHouseNode server, ClickHouseRequest<?> request,
            StreamObserver<QueryInfo> requestObserver, ClickHouseStreamObserver responseObserver) {
        this.server = ClickHouseChecker.nonNull(server, "server");
        this.request = ClickHouseChecker.nonNull(request, "request").seal();

        this.requestObserver = ClickHouseChecker.nonNull(requestObserver, "requestObserver");
        this.responseObserver = ClickHouseChecker.nonNull(responseObserver, "responseObserver");
    }

    public ClickHouseNode getServer() {
        return server;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancelled = true;

        if (mayInterruptIfRunning) {
            cancelled = Context.current().withCancellation().cancel(new StatusException(Status.CANCELLED));
        } else {
            requestObserver.onError(new StatusException(Status.CANCELLED));
        }

        return cancelled;
    }

    @Override
    public boolean isCancelled() {
        return responseObserver.isCancelled();
    }

    @Override
    public boolean isDone() {
        return responseObserver.isCompleted();
    }

    @Override
    public ClickHouseResponse get() throws InterruptedException, ExecutionException {
        try {
            return get(request.getConfig().getConnectionTimeout() / 1000 + request.getConfig().getMaxExecutionTime(),
                    TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            cancel(true);
            throw new InterruptedException(e.getMessage());
        }
    }

    @Override
    public ClickHouseResponse get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!responseObserver.await(timeout, unit)) {
            cancel(true);
            throw new TimeoutException(ClickHouseUtils.format("Timed out after waiting for %d %s", timeout, unit));
        }

        try {
            return new ClickHouseGrpcResponse(request.getConfig(), server, request.getSettings(), responseObserver);
        } catch (ClickHouseException e) {
            throw new ExecutionException(e);
        }
    }
}
