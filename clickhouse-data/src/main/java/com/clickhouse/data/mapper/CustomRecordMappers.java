package com.clickhouse.data.mapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseUtils;

@Deprecated
final class CustomRecordMappers {
    static final class RecordConstructor extends AbstractRecordMapper {
        private final Constructor<?> constructor;

        RecordConstructor(Class<?> objClass, Constructor<?> constructor) {
            super(objClass);
            this.constructor = constructor;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T mapTo(ClickHouseRecord r, Class<T> objClass, T obj) {
            return (T) newInstance(constructor, r);
        }
    }

    static final class RecordCreator extends AbstractRecordMapper {
        private final Method method;

        RecordCreator(Class<?> objClass, Method createMethod) {
            super(objClass);
            this.method = createMethod;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T mapTo(ClickHouseRecord r, Class<T> objClass, T obj) {
            if (method == null) {
                throw new IllegalArgumentException("No method to create instance");
            }

            try {
                return (T) method.invoke(null, r);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalArgumentException(
                        ClickHouseUtils.format("Failed to create instance of [%s] using [%s]", clazz.getName(), method),
                        e);
            }
        }
    }

    private CustomRecordMappers() {
    }
}