package com.clickhouse.benchmark.misc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import com.clickhouse.benchmark.BaseState;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.data.ClickHouseBigDecimalValue;
import com.clickhouse.client.data.ClickHouseBigIntegerValue;
import com.clickhouse.client.data.ClickHouseByteValue;
import com.clickhouse.client.data.ClickHouseDateTimeValue;
import com.clickhouse.client.data.ClickHouseDateValue;
import com.clickhouse.client.data.ClickHouseDoubleValue;
import com.clickhouse.client.data.ClickHouseEnumValue;
import com.clickhouse.client.data.ClickHouseFloatValue;
import com.clickhouse.client.data.ClickHouseIntegerValue;
import com.clickhouse.client.data.ClickHouseIpv4Value;
import com.clickhouse.client.data.ClickHouseIpv6Value;
import com.clickhouse.client.data.ClickHouseLongValue;
import com.clickhouse.client.data.ClickHouseShortValue;
import com.clickhouse.client.data.ClickHouseStringValue;
import com.clickhouse.client.data.ClickHouseTimeValue;

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
@Threads(value = -1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class FactoryBenchmark {
    @State(Scope.Thread)
    public static class FactoryState extends BaseState {
        public int samples;
        public Class<?> clazz;
        public Map<Class<?>, Supplier<? extends ClickHouseValue>> mappings;

        private List<Class<?>> classes;

        private static void add(Map<Class<?>, Supplier<? extends ClickHouseValue>> map, List<Class<?>> list,
                Class<?> clazz, Supplier<? extends ClickHouseValue> func) {
            if (map.put(clazz, func) != null) {
                throw new IllegalArgumentException("Duplicated key: " + clazz);
            }

            list.add(clazz);
        }

        @Setup(Level.Trial)
        public void setupSamples() {
            samples = 500000;
            Map<Class<?>, Supplier<? extends ClickHouseValue>> map = new HashMap<>();
            List<Class<?>> list = new ArrayList<>();
            add(map, list, Byte.class, () -> ClickHouseByteValue.ofNull());
            add(map, list, Short.class, () -> ClickHouseShortValue.ofNull());
            add(map, list, Integer.class, () -> ClickHouseIntegerValue.ofNull());
            add(map, list, Long.class, () -> ClickHouseLongValue.ofNull(false));
            add(map, list, Float.class, () -> ClickHouseFloatValue.ofNull());
            add(map, list, Double.class, () -> ClickHouseDoubleValue.ofNull());
            add(map, list, BigInteger.class, () -> ClickHouseBigIntegerValue.ofNull());
            add(map, list, BigDecimal.class, () -> ClickHouseBigDecimalValue.ofNull());
            add(map, list, Enum.class, () -> ClickHouseEnumValue.ofNull());
            add(map, list, Inet4Address.class, () -> ClickHouseIpv4Value.ofNull());
            add(map, list, Inet6Address.class, () -> ClickHouseIpv6Value.ofNull());

            // add(map, list, Object[].class, () -> ClickHouseArrayValue.of((Object[]) o));
            add(map, list, LocalDate.class, () -> ClickHouseDateValue.ofNull());
            add(map, list, LocalTime.class, () -> ClickHouseTimeValue.ofNull());
            add(map, list, LocalDateTime.class, () -> ClickHouseDateTimeValue.ofNull(0));
            add(map, list, String.class, () -> ClickHouseStringValue.ofNull());

            mappings = Collections.unmodifiableMap(map);
            classes = Collections.unmodifiableList(new ArrayList<>(list));
        }

        @Setup(Level.Iteration)
        public void initValueClass() {
            clazz = classes.get(getRandomNumber(classes.size()));
        }
    }

    static class IfElseFactory {
        ClickHouseValue newValue(Class<?> clazz) {
            if (Byte.class.equals(clazz)) {
                return ClickHouseByteValue.ofNull();
            } else if (Short.class.equals(clazz)) {
                return ClickHouseShortValue.ofNull();
            } else if (Integer.class.equals(clazz)) {
                return ClickHouseIntegerValue.ofNull();
            } else if (Long.class.equals(clazz)) {
                return ClickHouseLongValue.ofNull(false);
            } else if (Float.class.equals(clazz)) {
                return ClickHouseFloatValue.ofNull();
            } else if (Double.class.equals(clazz)) {
                return ClickHouseDoubleValue.ofNull();
            } else if (BigInteger.class.equals(clazz)) {
                return ClickHouseBigIntegerValue.ofNull();
            } else if (BigDecimal.class.equals(clazz)) {
                return ClickHouseBigDecimalValue.ofNull();
            } else if (Enum.class.equals(clazz)) {
                return ClickHouseEnumValue.ofNull();
            } else if (Inet4Address.class.equals(clazz)) {
                return ClickHouseIpv4Value.ofNull();
            } else if (Inet6Address.class.equals(clazz)) {
                return ClickHouseIpv6Value.ofNull();
            } else if (LocalDate.class.equals(clazz)) {
                return ClickHouseDateValue.ofNull();
            } else if (LocalTime.class.equals(clazz)) {
                return ClickHouseTimeValue.ofNull();
            } else if (LocalDateTime.class.equals(clazz)) {
                return ClickHouseDateTimeValue.ofNull(0);
            } else if (String.class.equals(clazz)) {
                return ClickHouseStringValue.ofNull();
            }

            throw new IllegalArgumentException("Unsupported type: " + clazz);
        }
    }

    static class HashMapFactory {
        ClickHouseValue newValue(Map<Class<?>, Supplier<? extends ClickHouseValue>> mappings, Class<?> clazz) {
            Supplier<? extends ClickHouseValue> func = mappings.get(clazz);
            if (func == null) {
                throw new IllegalArgumentException("Unsupported type: " + clazz);
            }
            return func.get();
        }
    }

    @Benchmark
    public void ifElse(FactoryState state, Blackhole consumer) {
        for (int i = 0; i < state.samples; i++) {
            IfElseFactory f = new IfElseFactory();
            consumer.consume(f.newValue(state.clazz));
        }
    }

    @Benchmark
    public void hashMap(FactoryState state, Blackhole consumer) {
        for (int i = 0; i < state.samples; i++) {
            HashMapFactory f = new HashMapFactory();
            consumer.consume(f.newValue(state.mappings, state.clazz));
        }
    }
}
