package com.clickhouse.r2dbc;

import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseRecord;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseResourceCleanupTest {

    @Test
    void closesResponseCompletedAfterCancellation() {
        CompletableFuture<ClickHouseResponse> future = new CompletableFuture<>();
        TestResponse response = new TestResponse(true);
        AtomicBoolean requestStarted = new AtomicBoolean();

        StepVerifier.create(ClickHouseStatement.executeRequest(() -> {
                    requestStarted.set(true);
                    return future;
                }))
                .then(() -> assertTrue(requestStarted.get()))
                .thenCancel()
                .verify();

        assertFalse(future.isCancelled());
        future.complete(response);

        assertTrue(response.isClosed());
        assertEquals(1, response.closeCount());
    }

    @Test
    void closesResponseCompletedWithoutDemandWhenCancelled() {
        CompletableFuture<ClickHouseResponse> future = new CompletableFuture<>();
        TestResponse response = new TestResponse(true);

        StepVerifier.create(ClickHouseStatement.executeRequest(() -> future), 0)
                .then(() -> future.complete(response))
                .thenCancel()
                .verify();

        assertTrue(response.isClosed());
        assertEquals(1, response.closeCount());
    }

    @Test
    void cancellationAndCompletionRaceDoesNotLoseResponseOwnership() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 500; i++) {
                CompletableFuture<ClickHouseResponse> future = new CompletableFuture<>();
                TestResponse response = new TestResponse(true);
                AtomicReference<Subscription> subscription = new AtomicReference<>();
                AtomicInteger received = new AtomicInteger();
                AtomicReference<Throwable> failure = new AtomicReference<>();

                ClickHouseStatement.executeRequest(() -> future).subscribe(new CoreSubscriber<ClickHouseResponse>() {
                    @Override
                    public void onSubscribe(Subscription value) {
                        subscription.set(value);
                        value.request(1);
                    }

                    @Override
                    public void onNext(ClickHouseResponse value) {
                        received.incrementAndGet();
                    }

                    @Override
                    public void onError(Throwable error) {
                        failure.set(error);
                    }

                    @Override
                    public void onComplete() {
                    }

                    @Override
                    public Context currentContext() {
                        return Context.empty();
                    }
                });

                CountDownLatch start = new CountDownLatch(1);
                Future<?> cancellation = executor.submit(() -> {
                    start.await();
                    subscription.get().cancel();
                    return null;
                });
                Future<?> completion = executor.submit(() -> {
                    start.await();
                    future.complete(response);
                    return null;
                });
                start.countDown();
                cancellation.get();
                completion.get();

                assertNull(failure.get());
                assertTrue(received.get() == 0 || received.get() == 1);
                assertEquals(received.get() == 0, response.isClosed());
                assertEquals(received.get() == 0 ? 1 : 0, response.closeCount());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void doesNotCloseResponseTransferredToSubscriber() {
        TestResponse response = new TestResponse(true);
        CompletableFuture<ClickHouseResponse> future = CompletableFuture.completedFuture(response);

        StepVerifier.create(ClickHouseStatement.executeRequest(() -> future))
                .expectNext(response)
                .verifyComplete();

        assertFalse(response.isClosed());
        assertEquals(0, response.closeCount());
    }

    @Test
    void closesResponseWhenResultConsumptionIsCancelled() {
        TestResponse response = new TestResponse(false);
        ClickHouseResult result = new ClickHouseResult(response);

        StepVerifier.create(result.map((row, metadata) -> 1), 1)
                .expectNext(1)
                .thenCancel()
                .verify();

        assertTrue(response.isClosed());
        assertEquals(1, response.closeCount());
    }

    @Test
    void closesResponseWhenResultConsumptionFails() {
        TestResponse response = new TestResponse(false, true);
        ClickHouseResult result = new ClickHouseResult(response);

        StepVerifier.create(result.map((row, metadata) -> 1), 1)
                .expectErrorMessage("Failed to read response")
                .verify();

        assertTrue(response.isClosed());
        assertEquals(1, response.closeCount());
    }

    private static final class TestResponse implements ClickHouseResponse {
        private static final long serialVersionUID = 1L;

        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicInteger closeCount = new AtomicInteger();
        private final boolean empty;
        private final boolean failOnRecords;

        TestResponse(boolean empty) {
            this(empty, false);
        }

        TestResponse(boolean empty, boolean failOnRecords) {
            this.empty = empty;
            this.failOnRecords = failOnRecords;
        }

        @Override
        public List<ClickHouseColumn> getColumns() {
            return Collections.emptyList();
        }

        @Override
        public ClickHouseResponseSummary getSummary() {
            return ClickHouseResponseSummary.EMPTY;
        }

        @Override
        public ClickHouseInputStream getInputStream() {
            return ClickHouseInputStream.empty();
        }

        @Override
        public Iterable<ClickHouseRecord> records() {
            if (empty) {
                return Collections.emptyList();
            }
            if (failOnRecords) {
                return () -> new Iterator<ClickHouseRecord>() {
                    @Override
                    public boolean hasNext() {
                        throw new IllegalStateException("Failed to read response");
                    }

                    @Override
                    public ClickHouseRecord next() {
                        throw new NoSuchElementException();
                    }
                };
            }
            return () -> new Iterator<ClickHouseRecord>() {
                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public ClickHouseRecord next() {
                    return ClickHouseRecord.EMPTY;
                }
            };
        }

        @Override
        public <T> Iterable<T> records(Class<T> objClass) {
            return Collections.emptyList();
        }

        @Override
        public void close() {
            closed.set(true);
            closeCount.incrementAndGet();
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        int closeCount() {
            return closeCount.get();
        }
    }
}
