package com.clickhouse.client.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpResponse.BodySubscriber;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseInputStream;

public class ClickHouseResponseHandler implements BodySubscriber<InputStream> {
    // An immutable ByteBuffer sentinel to mark that the last byte was received.
    private static final List<ByteBuffer> LAST_LIST = List.of(ClickHouseByteBuffer.EMPTY_BUFFER);

    private final BlockingQueue<ByteBuffer> buffers;
    private final ClickHouseInputStream in;
    private final AtomicBoolean subscribed;

    ClickHouseResponseHandler(int queueLength, int timeout) {
        buffers = queueLength > 1 ? new ArrayBlockingQueue<>(queueLength) : new LinkedBlockingQueue<>();
        in = ClickHouseInputStream.of(buffers, timeout);
        subscribed = new AtomicBoolean();
    }

    @Override
    public void onSubscribe(Subscription s) {
        try {
            if (!subscribed.compareAndSet(false, true)) {
                s.cancel();
            } else {
                if (in.isClosed()) {
                    s.cancel();
                    return;
                }
                s.request(Long.MAX_VALUE);
            }
        } catch (Throwable t) {
            try {
                in.close();
            } catch (IOException x) {
                // ignore
            } finally {
                onError(t);
            }
        }
    }

    @Override
    public void onNext(List<ByteBuffer> item) {
        try {
            if (!buffers.addAll(item)) {
                // should never happen
                throw new IllegalStateException("Queue is full");
            }
        } catch (Throwable t) {
            try {
                in.close();
            } catch (IOException e) {
                // ignore
            } finally {
                onError(t);
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        buffers.offer(ClickHouseByteBuffer.EMPTY_BUFFER);
    }

    @Override
    public void onComplete() {
        onNext(LAST_LIST);
    }

    @Override
    public CompletionStage<InputStream> getBody() {
        return CompletableFuture.completedStage(in);
    }
}
