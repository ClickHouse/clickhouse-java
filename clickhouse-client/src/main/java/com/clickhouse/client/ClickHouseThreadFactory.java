package com.clickhouse.client;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ClickHouseThreadFactory implements ThreadFactory {
    private final boolean daemon;
    private final int priority;

    private final ThreadGroup group;
    private final String namePrefix;
    private final AtomicInteger threadNumber;

    public ClickHouseThreadFactory(Object owner) {
        this(owner, true, Thread.NORM_PRIORITY);
    }

    public ClickHouseThreadFactory(Object owner, boolean daemon, int priority) {
        String prefix = null;
        if (owner instanceof String) {
            prefix = ((String) owner).trim();
        } else if (owner != null) {
            prefix = new StringBuilder().append(owner.getClass().getSimpleName()).append('@').append(owner.hashCode())
                    .toString();
        }
        this.daemon = daemon;
        this.priority = ClickHouseChecker.between(priority, "Priority", Thread.MIN_PRIORITY, Thread.MAX_PRIORITY);

        SecurityManager s = System.getSecurityManager();
        group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namePrefix = !ClickHouseChecker.isNullOrBlank(prefix) ? prefix
                : new StringBuilder().append(getClass().getSimpleName()).append('@').append(hashCode())
                        .append('-').toString();
        threadNumber = new AtomicInteger(1);
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        if (daemon != t.isDaemon()) {
            t.setDaemon(daemon);
        }
        if (priority != t.getPriority()) {
            t.setPriority(priority);
        }
        // t.setUncaughtExceptionHandler(null);
        return t;
    }
}
