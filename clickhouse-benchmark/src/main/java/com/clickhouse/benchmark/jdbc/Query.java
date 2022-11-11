package com.clickhouse.benchmark.jdbc;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class Query extends DriverBenchmark {
    @Benchmark
    public void selectArrayOfUInt16(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getArray(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state,
                "select range(100, number % 600) as v from numbers(?)", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectMapOfInt32(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getObject(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state,
                "select cast((arrayMap(x->x+1000, range(1, number % 100)), arrayMap(x->x+10000, range(1, number %100))) as Map(Int32, Int32)) as v from numbers(?)",
                rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectTupleOfInt16(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getObject(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state,
                "select tuple(arrayMap(x -> cast(x as Int16), range(100, number % 600))) as v from numbers(?)", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectDateTime32(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getTimestamp(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state,
                "select toDateTime32(1613826920 + number) as v from numbers(?)", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectDateTime64(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getTimestamp(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state,
                "select toDateTime64(1613826920 + number / 1000000000, 9) as v from numbers(?)", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectInt8(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getByte(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state, "select toInt8(number % 256) as v from numbers(?)", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectUInt8(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getByte(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state, "select toUInt8(number % 256) as v from numbers(?)", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectUuid(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getString(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state, "select generateUUIDv4() as v from numbers(?)", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectInt32(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getInt(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state, "select toInt32(number) as v from numbers(?)", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectUInt32(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getInt(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state, "select toUInt32(number) as v from numbers(?)", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectString(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getString(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state, "select toString(number/3) as v from numbers(?)", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectUInt64(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getLong(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state, "select number as v from numbers(?)", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }

    @Benchmark
    public void selectDecimal64(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        ConsumeValueFunction func = state.getConsumeFunction((b, r, l, i) -> b.consume(r.getBigDecimal(i)));
        int l = 0;
        try (Statement stmt = executeQuery(state, "select toDecimal64(number + number / 10000, 4) as v from numbers(?)",
                rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                func.consume(blackhole, rs, l++, 1);
            }
        }
        if (l != rows) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Expected %d rows but got %d", rows, l));
        }
    }
}
