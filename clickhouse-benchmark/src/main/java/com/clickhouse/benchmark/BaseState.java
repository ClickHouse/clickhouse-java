package com.clickhouse.benchmark;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.infra.Blackhole;

public abstract class BaseState {
    // avoid server-side cache
    private final Random random;

    // sync vs async
    private final Semaphore available;
    private final ExecutorService executor;

    public BaseState() {
        random = new Random();

        int consumers = Integer.parseInt(System.getProperty("consumers", "0"));

        if (consumers > 0) {
            available = new Semaphore(consumers);
            executor = Executors.newSingleThreadExecutor();
        } else {
            available = null;
            executor = null;
        }
    }

    protected int getRandomNumber(int bound) {
        return bound < 1 ? 0 : random.nextInt(bound);
    }

    protected void consume(Blackhole blackhole, Callable<?> task) throws InterruptedException {
        if (available != null) {
            available.acquire();

            executor.submit(() -> {
                try {
                    blackhole.consume(task.call());
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                } finally {
                    available.release();
                }
            });
        } else {
            try {
                blackhole.consume(task.call());
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    protected void dispose() {
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
