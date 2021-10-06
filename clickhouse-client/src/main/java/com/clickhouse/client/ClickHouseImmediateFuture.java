package com.clickhouse.client;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Encapsulates a {@link java.util.concurrent.Callable} when
 * {@link com.clickhouse.client.config.ClickHouseClientOption#ASYNC} is set to
 * {@code false}. It's not cancellable and the task will be only executed when
 * {@link #get()} is called.
 * 
 * @deprecated
 */
@Deprecated
public final class ClickHouseImmediateFuture<T> implements Future<T> {
    public static <T> Future<T> of(Callable<T> task) {
        return new ClickHouseImmediateFuture<>(task);
    }

    private final Callable<T> task;

    private ClickHouseImmediateFuture(Callable<T> task) {
        this.task = ClickHouseChecker.nonNull(task, "task");
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        try {
            return task.call();
        } catch (InterruptedException | ExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        ClickHouseChecker.nonNull(unit, "unit");
        return get();
    }
}
