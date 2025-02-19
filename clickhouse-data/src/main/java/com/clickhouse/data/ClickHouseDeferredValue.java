package com.clickhouse.data;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * This class represents a deferred value. It holds a reference to a
 * {@link Supplier} or {@link Future} to retrieve value only when {@link #get()}
 * or {@link #getOptional()} was called.
 */
@Deprecated
public final class ClickHouseDeferredValue<T> implements Supplier<T> {
    /**
     * Wraps a future object.
     *
     * @param <T>    type of the value
     * @param future future object, could be null
     * @return deferred value of a future object
     */
    public static <T> ClickHouseDeferredValue<T> of(CompletableFuture<T> future) {
        return of(future, 0L);
    }

    /**
     * Wraps a future object.
     *
     * @param <T>     type of the value
     * @param future  future object, could be null
     * @param timeout timeout in milliseconds, zero or negative number means no
     *                timeout
     * @return deferred vaue of a future object
     */
    @SuppressWarnings("unchecked")
    public static <T> ClickHouseDeferredValue<T> of(CompletableFuture<T> future, long timeout) {
        final CompletableFuture<T> f = future != null ? future : (CompletableFuture<T>) ClickHouseUtils.NULL_FUTURE;
        final long t = timeout < 0L ? 0L : timeout;

        final Supplier<T> supplier = () -> {
            try {
                return t > 0L ? f.get(t, TimeUnit.MILLISECONDS) : f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                f.cancel(false);
                throw new CompletionException(e);
            } catch (CancellationException e) {
                f.cancel(true);
                throw new CompletionException(e);
            } catch (ExecutionException | TimeoutException e) {
                throw new CompletionException(e);
            }
        };
        return new ClickHouseDeferredValue<>(supplier, null);
    }

    /**
     * Wraps return value from a supplier function.
     *
     * @param <T>      type of the value
     * @param supplier supplier function, could be null
     * @return deferred value of return value from supplier function
     */
    @SuppressWarnings("unchecked")
    public static <T> ClickHouseDeferredValue<T> of(Supplier<T> supplier) {
        return new ClickHouseDeferredValue<>(supplier != null ? supplier : (Supplier<T>) ClickHouseUtils.NULL_SUPPLIER,
                null);
    }

    /**
     * Wraps given value as a deferred value.
     *
     * @param <T>   type of the value
     * @param value value to wrap
     * @param clazz class of the value
     * @return deferred value
     */
    @SuppressWarnings("unchecked")
    public static <T> ClickHouseDeferredValue<T> of(T value, Class<T> clazz) { // NOSONAR
        return new ClickHouseDeferredValue<>((Supplier<T>) ClickHouseUtils.NULL_SUPPLIER, Optional.ofNullable(value));
    }

    private final Supplier<T> supplier;
    private final AtomicReference<Optional<T>> value;

    private ClickHouseDeferredValue(Supplier<T> supplier, Optional<T> value) {
        this.supplier = supplier;
        this.value = new AtomicReference<>(value);
    }

    @Override
    public T get() {
        return getOptional().orElse(null);
    }

    /**
     * Gets optional value, which may or may not be null.
     *
     * @return optional value
     */
    public Optional<T> getOptional() {
        Optional<T> v = value.get();
        if (v == null && !value.compareAndSet(null, v = Optional.ofNullable(supplier.get()))) { // NOSONAR
            v = value.get();
        }

        return v != null ? v : Optional.empty(); // NOSONAR
    }
}
