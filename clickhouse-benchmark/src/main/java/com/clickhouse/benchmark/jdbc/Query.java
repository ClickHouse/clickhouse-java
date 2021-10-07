package com.clickhouse.benchmark.jdbc;

import java.sql.ResultSet;
import java.sql.Statement;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class Query extends DriverBenchmark {
    @Benchmark
    public void selectDateTime32Rows(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        try (Statement stmt = executeQuery(state,
                "select toDateTime('2021-02-20 13:15:20') + number as d from system.numbers limit ?", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                blackhole.consume(rs.getTimestamp(1));
            }
        }
    }

    @Benchmark
    public void selectDateTime64Rows(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        try (Statement stmt = executeQuery(state,
                "select toDateTime64('2021-02-20 13:15:20.000000000', 9) + number as d from system.numbers limit ?",
                rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                blackhole.consume(rs.getTimestamp(1));
            }
        }
    }

    @Benchmark
    public void selectInt32Rows(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        try (Statement stmt = executeQuery(state, "select * from system.numbers limit ?", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                blackhole.consume(rs.getInt(1));
            }
        }
    }

    @Benchmark
    public void selectStringRows(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        try (Statement stmt = executeQuery(state, "select toString(number) as s from system.numbers limit ?", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                blackhole.consume(rs.getString(1));
            }
        }
    }

    @Benchmark
    public void selectUInt64Rows(Blackhole blackhole, DriverState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        try (Statement stmt = executeQuery(state, "select * from system.numbers limit ?", rows)) {
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                blackhole.consume(rs.getLong(1));
            }
        }
    }
}
