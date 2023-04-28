package com.clickhouse.data.mapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import com.clickhouse.data.ClickHouseCache;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseRecordMapper;

public final class RecordMapperFactory {
    private static final ClickHouseCache<Class<?>, WrappedMapper> CACHED_MAPPERS;
    private static final boolean USE_COMPILED_MAPPER;

    static {
        CACHED_MAPPERS = ClickHouseCache.create(100, 0, RecordMapperFactory::get);

        Class<?> asmClass = null;
        try {
            asmClass = Class.forName("org.objectweb.asm.ClassWriter", true,
                    RecordMapperFactory.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            // ignore
        }
        USE_COMPILED_MAPPER = asmClass != null;
    }

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

        // return USE_COMPILED_MAPPER ? new CompiledRecordMapper(objClass) : new DynamicRecordMapper(objClass);
        return new DynamicRecordMapper(objClass);
    }

    public static ClickHouseRecordMapper of(List<ClickHouseColumn> columns, Class<?> objClass) {
        if (columns == null || objClass == null) {
            throw new IllegalArgumentException("Non-null column list and object class are required");
        }

        return CACHED_MAPPERS.get(objClass).get(columns);
    }

    private RecordMapperFactory() {
    }
}