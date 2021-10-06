package com.clickhouse.client.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.exception.ClickHouseException;
import com.clickhouse.client.grpc.impl.ClickHouseGrpc;
import com.clickhouse.client.grpc.impl.ExternalTable;
import com.clickhouse.client.grpc.impl.NameAndType;
import com.clickhouse.client.grpc.impl.QueryInfo;
import com.clickhouse.client.grpc.impl.Result;
import com.clickhouse.client.grpc.impl.QueryInfo.Builder;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

public class ClickHouseGrpcClient implements ClickHouseClient {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseGrpcClient.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // not going to offload executor to ClickHouseGrpcFuture, as we can manage
    // thread pool better here
    private final AtomicReference<ExecutorService> executor = new AtomicReference<>();

    // do NOT use below members directly without ReadWriteLock
    private final AtomicReference<ClickHouseConfig> config = new AtomicReference<>();
    private final AtomicReference<ClickHouseNode> server = new AtomicReference<>();
    private final AtomicReference<ManagedChannel> channel = new AtomicReference<>();

    protected static QueryInfo convert(ClickHouseNode server, ClickHouseRequest<?> request) {
        ClickHouseConfig config = request.getConfig();
        ClickHouseCredentials credentials = server.getCredentials().orElse(config.getDefaultCredentials());

        Builder builder = QueryInfo.newBuilder();
        builder.setDatabase(server.getDatabase()).setUserName(credentials.getUserName())
                .setPassword(credentials.getPassword()).setOutputFormat(request.getFormat().name());

        Optional<String> optionalValue = request.getSessionId();
        if (optionalValue.isPresent()) {
            builder.setSessionId(optionalValue.get());
        }
        if (config.isSessionCheck()) {
            builder.setSessionCheck(true);
        }
        if (config.getSessionTimeout() > 0) {
            builder.setSessionTimeout(config.getSessionTimeout());
        }

        optionalValue = request.getQueryId();
        if (optionalValue.isPresent()) {
            builder.setQueryId(optionalValue.get());
        }

        // builder.setNextQueryInfo(true);
        for (Entry<String, Object> s : request.getSettings().entrySet()) {
            builder.putSettings(s.getKey(), String.valueOf(s.getValue()));
        }

        Optional<InputStream> input = request.getInputStream();
        if (input.isPresent()) {
            try {
                builder.setInputData(ByteString.readFrom(input.get()));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        List<ClickHouseExternalTable> externalTables = request.getExternalTables();
        if (!externalTables.isEmpty()) {
            for (ClickHouseExternalTable external : externalTables) {
                ExternalTable.Builder b = ExternalTable.newBuilder().setName(external.getName());
                for (ClickHouseColumn c : ClickHouseColumn.parse(external.getStructure())) {
                    b.addColumns(NameAndType.newBuilder().setName(c.getColumnName()).setType(c.getOriginalTypeName())
                            .build());
                }
                if (external.getFormat() != null) {
                    b.setFormat(external.getFormat().name());
                }

                try {
                    builder.addExternalTables(b.setData(ByteString.readFrom(external.getContent())).build());
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        List<String> stmts = request.getStatements(false);
        int size = stmts.size();
        String sql;
        if (size == 0) {
            throw new IllegalArgumentException("At least one SQL statement is required for execution");
        } else if (size == 1) {
            sql = stmts.get(0);
        } else { // consolidate statements into one
            if (!builder.getSessionCheck()) {
                builder.setSessionCheck(true);
            }

            if (ClickHouseChecker.isNullOrEmpty(builder.getSessionId())) {
                builder.setSessionId(UUID.randomUUID().toString());
            }

            // builder.getSessionTimeout()
            StringBuilder sb = new StringBuilder();
            for (String s : stmts) {
                sb.append(s).append(';').append('\n');
            }
            sql = sb.toString();
        }

        log.debug("Query: %s", sql);

        return builder.setQuery(sql).build();
    }

    protected void fill(ClickHouseRequest<?> request, StreamObserver<QueryInfo> observer) {
        try {
            observer.onNext(convert(getServer(), request));
        } finally {
            observer.onCompleted();
        }
    }

    protected ClickHouseNode getServer() {
        lock.readLock().lock();
        try {
            return server.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    protected ManagedChannel getChannel(ClickHouseRequest<?> request) {
        boolean prepared = true;
        ClickHouseNode newNode = ClickHouseChecker.nonNull(request, "request").getServer();

        lock.readLock().lock();
        ManagedChannel c = channel.get();
        try {
            prepared = c != null && newNode.equals(server.get());

            if (prepared) {
                return c;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        c = channel.get();
        try {
            // first time?
            if (c == null) {
                server.set(newNode);
                channel.set(c = ClickHouseGrpcChannelFactory.getFactory(getConfig(), newNode).create());
            } else if (!newNode.equals(server.get())) {
                log.debug("Shutting down channel: %s", c);
                c.shutdownNow();

                server.set(newNode);
                channel.set(c = ClickHouseGrpcChannelFactory.getFactory(getConfig(), newNode).create());
            }

            return c;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean accept(ClickHouseProtocol protocol) {
        return ClickHouseProtocol.GRPC == protocol || ClickHouseClient.super.accept(protocol);
    }

    @Override
    public ClickHouseConfig getConfig() {
        lock.readLock().lock();
        try {
            return config.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void init(ClickHouseConfig config) {
        lock.writeLock().lock();
        try {
            this.config.set(config);
            ClickHouseClient.super.init(config);
            if (this.executor.get() == null) { // only initialize once
                int threads = config.getMaxThreadsPerClient();
                this.executor.set(threads <= 0 ? ClickHouseClient.getExecutorService()
                        : ClickHouseUtils.newThreadPool(ClickHouseGrpcClient.class.getSimpleName(), threads,
                                config.getMaxQueuedRequests()));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();

        ExecutorService s = executor.get();
        ManagedChannel m = channel.get();

        try {
            server.set(null);

            if (s != null) {
                s.shutdown();
                executor.set(null);
            }

            if (m != null) {
                m.shutdown();
                channel.set(null);
            }

            ClickHouseConfig c = config.get();
            if (c != null) {
                config.set(null);
            }

            // shutdown* won't shutdown commonPool, so awaitTermination will always time out
            // on the other hand, for a client-specific thread pool, we'd better shut it
            // down for real
            if (s != null && c != null && c.getMaxThreadsPerClient() > 0
                    && !s.awaitTermination((int) c.getOption(ClickHouseClientOption.CONNECTION_TIMEOUT),
                            TimeUnit.MILLISECONDS)) {
                s.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.warn("Exception occurred when closing client", e);
        } finally {
            try {
                if (m != null) {
                    m.shutdownNow();
                }

                if (s != null) {
                    s.shutdownNow();
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    protected CompletableFuture<ClickHouseResponse> executeAsync(ClickHouseRequest<?> sealedRequest,
            ManagedChannel channel, ClickHouseNode server) {
        // reuse stub?
        ClickHouseGrpc.ClickHouseStub stub = ClickHouseGrpc.newStub(channel);
        stub.withCompression(sealedRequest.getCompression().encoding());

        final ClickHouseStreamObserver responseObserver = new ClickHouseStreamObserver(sealedRequest.getConfig(),
                server);
        final StreamObserver<QueryInfo> requestObserver = stub.executeQueryWithStreamIO(responseObserver);

        if (sealedRequest.hasInputStream()) {
            executor.get().execute(() -> fill(sealedRequest, requestObserver));
        } else {
            fill(sealedRequest, requestObserver);
        }

        // return new ClickHouseGrpcFuture(server, sealedRequest, requestObserver,
        // responseObserver);
        return CompletableFuture.supplyAsync(() -> {
            int timeout = sealedRequest.getConfig().getConnectionTimeout() / 1000
                    + sealedRequest.getConfig().getMaxExecutionTime();
            try {
                if (!responseObserver.await(timeout, TimeUnit.SECONDS)) {
                    if (!Context.current().withCancellation().cancel(new StatusException(Status.CANCELLED))) {
                        requestObserver.onError(new StatusException(Status.CANCELLED));
                    }
                    throw new CompletionException(
                            ClickHouseUtils.format("Timed out after waiting for %d %s", timeout, TimeUnit.SECONDS),
                            null);
                }
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }

            try {
                return new ClickHouseGrpcResponse(sealedRequest.getConfig(), server, sealedRequest.getSettings(),
                        responseObserver);
            } catch (ClickHouseException e) {
                throw new CompletionException(e);
            }
        }, executor.get());
    }

    protected CompletableFuture<ClickHouseResponse> executeSync(ClickHouseRequest<?> sealedRequest,
            ManagedChannel channel, ClickHouseNode server) throws ClickHouseException {
        ClickHouseGrpc.ClickHouseBlockingStub stub = ClickHouseGrpc.newBlockingStub(channel);
        stub.withCompression(sealedRequest.getCompression().encoding());
        Result result = stub.executeQuery(convert(server, sealedRequest));

        // TODO not as elegant as ClickHouseImmediateFuture :<
        return CompletableFuture.completedFuture(
                new ClickHouseGrpcResponse(sealedRequest.getConfig(), server, sealedRequest.getSettings(), result));
    }

    @Override
    public CompletableFuture<ClickHouseResponse> execute(ClickHouseRequest<?> request) throws ClickHouseException {
        // sealedRequest is an immutable copy of the original request
        final ClickHouseRequest<?> sealedRequest = request.seal();
        final ManagedChannel c = getChannel(sealedRequest);
        final ClickHouseNode s = getServer();

        return sealedRequest.getConfig().isAsync() ? executeAsync(sealedRequest, c, s)
                : executeSync(sealedRequest, c, s);
    }
}
