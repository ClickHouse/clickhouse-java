package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.metrics.Metric;

import java.util.concurrent.TimeUnit;

public class StopWatch implements Metric {

    long elapsedNanoTime = 0;
    long startNanoTime;

    public StopWatch() {
        // do nothing
    }

    public StopWatch(long startNanoTime) {
        this.startNanoTime = startNanoTime;
    }

    public void start() {
        startNanoTime = System.nanoTime();
    }

    public void stop() {
        elapsedNanoTime = System.nanoTime() - startNanoTime;
    }

    /**
     * Returns the elapsed time in milliseconds.
     * @return
     */
    public long getElapsedTime() {
        return TimeUnit.NANOSECONDS.toMillis(elapsedNanoTime);
    }

    @Override
    public String toString() {
        return "{" +
                "\"elapsedNanoTime\"=" + elapsedNanoTime +
                ", \"elapsedTime\"=" + getElapsedTime() +
                '}';
    }

    @Override
    public long getLong() {
        return getElapsedTime();
    }
}
