package com.clickhouse.data;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * This class represents a deferred value. It holds a reference to a
 * {@link Supplier} or {@link Future} to retrieve value only when {@link #get()}
 * or {@link #getOptional()} was called.
 */
public final class ClickHouseDeferredValue<T> implements Supplier<T> {
    /**
     * Wraps a future object.
     *
     * @param <T>    type of the value
     * @param future future object, could be null
     * @return deferred value of a future object
     */
    public static <T> ClickHouseDeferredValue<T> of(CompletableFuture<T> future) {
        return of(future, 0);
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
    public static <T> ClickHouseDeferredValue<T> of(CompletableFuture<T> future, int timeout) {
        final CompletableFuture<T> f = future != null ? future : CompletableFuture.completedFuture(null);
        final int t = timeout < 0 ? 0 : timeout;

        Supplier<T> supplier = () -> {
            try {
                return f.get(t, TimeUnit.MILLISECONDS);
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
    public static <T> ClickHouseDeferredValue<T> of(Supplier<T> supplier) {
        return new ClickHouseDeferredValue<>(supplier != null ? supplier : () -> null, null);
    }

    /**
     * Wraps given value as a deferred value.
     *
     * @param <T>   type of the value
     * @param value value to wrap
     * @param clazz class of the value
     * @return deferred value
     */
    public static <T> ClickHouseDeferredValue<T> of(T value, Class<T> clazz) { // NOSONAR
        return new ClickHouseDeferredValue<>(null, Optional.ofNullable(value));
    }

    private Supplier<T> supplier;
    private Optional<T> value;

    private ClickHouseDeferredValue(Supplier<T> supplier, Optional<T> value) {
        this.supplier = supplier;
        this.value = value;
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
        if (value != null) { // NOSONAR
            return value;
        }

        return value = (supplier == null ? Optional.empty() : Optional.ofNullable(supplier.get())); // NOSONAR
    }
}
