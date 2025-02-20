package com.clickhouse.data.mapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseRecordMapper;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;

@Deprecated
final class DynamicRecordMapper extends AbstractRecordMapper {
    static class PropertySetter implements BiConsumer<Object, ClickHouseValue> {
        private static final Map<String, Method> typedValues;

        static {
            Map<String, Method> map = new HashMap<>();
            for (Method m : ClickHouseValue.class.getMethods()) {
                int modifiers = m.getModifiers();
                String name = m.getName();
                if (Modifier.isPublic(modifiers) && !Modifier.isStatic(modifiers) && m.getParameterCount() == 0
                        && name.startsWith("as")) {
                    Class<?> type = m.getReturnType();
                    if (type != Void.class) {
                        map.put(type.getName(), m);
                    }
                }
            }

            typedValues = Collections.unmodifiableMap(map);
        }

        protected final int index;
        protected final Class<?> type;
        protected final Method getter;
        protected final Method setter;

        protected PropertySetter(int valueIndex, Class<?> valueType, Method s) {
            index = valueIndex;
            type = valueType;
            getter = typedValues.get(valueType.getName());
            setter = s;
        }

        @Override
        public void accept(Object obj, ClickHouseValue val) {
            try {
                setter.invoke(obj, getter != null ? getter.invoke(val) : val.asObject(type));
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        ClickHouseUtils.format("Failed to set [%s] due to %s", type.getName(), e.getMessage()));
            }
        }
    }

    static class ObjectSetter extends PropertySetter {
        ObjectSetter(int valueIndex, Method s) {
            super(valueIndex, Object.class, s);
        }

        @Override
        public void accept(Object obj, ClickHouseValue val) {
            try {
                // asRawObject() might be impacted by use_binary_string option
                setter.invoke(obj, val.asObject());
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        ClickHouseUtils.format("Failed to set %s due to %s", val, e.getMessage()));
            }
        }
    }

    static class ValueSetter extends PropertySetter {
        ValueSetter(int valueIndex, Class<?> valueType, Method s) {
            super(valueIndex, valueType, s);
        }

        @Override
        public void accept(Object obj, ClickHouseValue val) {
            try {
                setter.invoke(obj, val);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        ClickHouseUtils.format("Failed to set %s due to %s", val, e.getMessage()));
            }
        }
    }

    private final Constructor<?> constructor;
    private final PropertySetter[] setters;

    protected DynamicRecordMapper(Class<?> objClass) {
        super(objClass);

        this.constructor = getDefaultConstructor(objClass);
        this.setters = new PropertySetter[0];
    }

    protected DynamicRecordMapper(Class<?> objClass, List<ClickHouseColumn> columns,
            Constructor<?> defaultConstructor) {
        super(objClass);

        this.constructor = defaultConstructor;

        PropertyInfo[] properties = getProperties(objClass, columns);
        int len = properties.length;
        List<PropertySetter> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            PropertyInfo p = properties[i];
            Class<?> type = p.setter.getParameterTypes()[0];
            if (Object.class == type) {
                list.add(new ObjectSetter(p.index, p.setter));
            } else if (ClickHouseValue.class.isAssignableFrom(type)) {
                list.add(new ValueSetter(p.index, type, p.setter));
            } else {
                list.add(new PropertySetter(p.index,
                        !type.isPrimitive() && !PropertySetter.typedValues.containsKey(type.getName())
                                ? ClickHouseDataType.toPrimitiveType(type)
                                : type,
                        p.setter));
            }
        }
        this.setters = list.toArray(new PropertySetter[0]);
    }

    @Override
    public ClickHouseRecordMapper get(ClickHouseDataConfig config, List<ClickHouseColumn> columns) {
        return new DynamicRecordMapper(clazz, columns == null ? Collections.emptyList() : columns, constructor);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T mapTo(ClickHouseRecord r, Class<T> objClass, T obj) {
        check(r, objClass);

        if (obj == null) {
            obj = (T) newInstance(constructor);
        }

        try {
            for (int i = 0, len = setters.length; i < len; i++) {
                PropertySetter setter = setters[i];
                setter.accept(obj, r.getValue(setter.index));
            }
            return obj;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to map record to specified class", e);
        }
    }
}
