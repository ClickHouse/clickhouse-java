package com.clickhouse.data.mapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.clickhouse.data.ClickHouseCache;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseRecordMapper;
import com.clickhouse.data.UnloadableClassLoader;

@Deprecated
public final class RecordMapperFactory {
    private static final AtomicReference<ClickHouseCache<Class<?>, WrappedMapper>> CACHED_MAPPERS = new AtomicReference<>();

    static WrappedMapper get(Class<?> objClass) {
        Constructor<?> preferredConsutrctor = null;
        try {
            for (Constructor<?> c : objClass.getDeclaredConstructors()) {
                // varargs are not supported
                if (c.getParameterCount() == 1 && ClickHouseRecord.class.isAssignableFrom(c.getParameterTypes()[0])) {
                    preferredConsutrctor = c;
                    break;
                }
            }
        } catch (SecurityException e) {
            // ignore
        }

        if (preferredConsutrctor != null) {
            return new CustomRecordMappers.RecordConstructor(objClass, preferredConsutrctor);
        } else {
            try {
                for (Method method : objClass.getDeclaredMethods()) {
                    int modifiers = method.getModifiers();
                    // varargs are not supported
                    if (Modifier.isStatic(modifiers) && method.getParameterCount() == 1
                            && objClass.isAssignableFrom(method.getReturnType())
                            && ClickHouseRecord.class.isAssignableFrom(method.getParameterTypes()[0])) {
                        return new CustomRecordMappers.RecordCreator(objClass, method);
                    }
                }
            } catch (SecurityException e) {
                // ignore
            }
        }

        return UnloadableClassLoader.HAS_ASM ? new CompiledRecordMapper(objClass) : new DynamicRecordMapper(objClass);
    }

    public static ClickHouseRecordMapper of(ClickHouseDataConfig config, List<ClickHouseColumn> columns,
            Class<?> objClass) {
        if (columns == null || objClass == null) {
            throw new IllegalArgumentException("Non-null column list and object class are required");
        }

        ClickHouseCache<Class<?>, WrappedMapper> cache = CACHED_MAPPERS.get();
        if (cache == null) {
            cache = ClickHouseCache.create(config != null ? config.getMaxMapperCache()
                    : ClickHouseDataConfig.DEFAULT_MAX_MAPPER_CACHE, 0, RecordMapperFactory::get);
            if (!CACHED_MAPPERS.compareAndSet(null, cache)) {
                cache = CACHED_MAPPERS.get();
            }
        }

        return cache.get(objClass).get(config, columns);
    }

    private RecordMapperFactory() {
    }
}