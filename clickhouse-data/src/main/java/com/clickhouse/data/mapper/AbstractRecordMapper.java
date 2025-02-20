package com.clickhouse.data.mapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseRecordMapper;
import com.clickhouse.data.ClickHouseUtils;

@Deprecated
public abstract class AbstractRecordMapper implements ClickHouseRecordMapper, WrappedMapper {

    static final class PropertyInfo {
        final int index;
        final ClickHouseColumn column;
        final Method setter;

        PropertyInfo(int index, ClickHouseColumn column, Method setter) {
            this.index = index;
            this.column = column;
            this.setter = setter;
        }
    }

    static final Constructor<?> getDefaultConstructor(Class<?> objClass) {
        Constructor<?> c = null;
        try {
            c = objClass.getDeclaredConstructor();
        } catch (NoSuchMethodException | SecurityException e) {
            // doesn't matter
        }
        return c;
    }

    static final Method[] getSetterMethods(Class<?> objClass) {
        List<Method> list;
        try {
            // public methods only
            Method[] methods = objClass.getMethods();
            int size = methods.length;
            list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                Method m = methods[i];
                // return type doesn't matter
                if (!Modifier.isStatic(m.getModifiers()) && m.getParameterCount() == 1
                        && m.getName().startsWith("set")) {
                    list.add(m);
                }
            }
        } catch (SecurityException e) {
            list = Collections.emptyList();
        }
        return list.toArray(new Method[0]);
    }

    static final PropertyInfo[] getProperties(Class<?> objClass, List<ClickHouseColumn> columns) {
        Method[] setters = getSetterMethods(objClass);
        int len = setters.length;
        int size = columns.size();
        List<PropertyInfo> list = new ArrayList<>(Math.min(size, setters.length));
        for (int i = 0; i < size; i++) {
            final ClickHouseColumn c = columns.get(i);
            final String name;
            if (c == null || ClickHouseChecker.isNullOrEmpty(name = c.getColumnName())) {
                continue;
            }

            final String setter = "set".concat(name);
            final String alias = ClickHouseUtils.remove(setter, '_', '-', ' ', '\t', '\r', '\n', '\'', '"', '`');
            for (int j = 0; j < len; j++) {
                Method m = setters[j];
                String n = m.getName();
                if (setter.equalsIgnoreCase(n) || (!alias.equals(setter) && alias.equalsIgnoreCase(n))) {
                    list.add(new PropertyInfo(i, c, m));
                    break;
                }
            }
        }
        return list.toArray(new PropertyInfo[0]);
    }

    static final Object newInstance(Constructor<?> constructor, Object... args) {
        if (constructor == null) {
            throw new IllegalArgumentException("No constructor available to create instance");
        }

        try {
            return constructor.newInstance(args);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalArgumentException(
                    ClickHouseUtils.format("Failed to instantiate [%s] with args [%s]",
                            constructor.getDeclaringClass().getName(), args, e));
        }
    }

    protected final Class<?> clazz;

    protected AbstractRecordMapper(Class<?> objClass) {
        this.clazz = objClass;
    }

    protected void check(ClickHouseRecord r, Class<?> objClass) {
        // if (clazz != objClass) {
        // throw new IllegalArgumentException(
        // ClickHouseUtils.format("Only supports class [%s] but we got [%s]", clazz,
        // objClass));
        // }
        if (r == null) {
            throw new IllegalArgumentException("Non-null record is required");
        }
    }

    @Override
    public ClickHouseRecordMapper get(ClickHouseDataConfig config, List<ClickHouseColumn> columns) {
        return this;
    }
}