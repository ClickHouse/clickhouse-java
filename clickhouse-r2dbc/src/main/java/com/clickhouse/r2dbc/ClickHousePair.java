package com.clickhouse.r2dbc;

import java.io.Serializable;
import java.util.Map.Entry;

/**
 * Immutable pair with two values: left and right.
 */
public class ClickHousePair<L, R> implements Entry<L, R>, Serializable {
    public static final ClickHousePair<?, ?> EMPTY = new ClickHousePair<>(null, null);

    @SuppressWarnings("unchecked")
    public static final <L, R> ClickHousePair<L, R> of(L left, R right) {
        if (left == null && right == null) {
            return (ClickHousePair<L, R>) EMPTY;
        }

        return new ClickHousePair<>(left, right);
    }

    private final L left;
    private final R right;

    public L getLeft() {
        return left;
    }

    public R getRight() {
        return right;
    }

    protected ClickHousePair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public L getKey() {
        return left;
    }

    @Override
    public R getValue() {
        return right;
    }

    @Override
    public R setValue(R value) {
        throw new IllegalStateException("Cannot modify value");
    }
}
