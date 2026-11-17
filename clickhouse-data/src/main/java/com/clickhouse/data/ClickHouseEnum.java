package com.clickhouse.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;


public class ClickHouseEnum implements Serializable {
    private static final long serialVersionUID = -978231193821209918L;

    public static final ClickHouseEnum EMPTY = new ClickHouseEnum(Collections.emptyList());

    public static ClickHouseEnum of(Class<? extends Enum> clazz) {
        if (clazz == null || !Enum.class.isAssignableFrom(clazz)) {
            return EMPTY;
        }

        Enum<?>[] constants = clazz.getEnumConstants();
        int size = constants.length;
        String[] names = new String[size];
        int[] values = new int[size];
        int i = 0;
        for (Enum<?> e : clazz.getEnumConstants()) {
            names[i] = e.name();
            values[i] = e.ordinal();
            i++;
        }

        return new ClickHouseEnum(names, values);
    }

    private final int size;
    private final String[] names;
    private final int[] values;

    protected ClickHouseEnum(Collection<String> params) {
        size = params.size();
        names = new String[size];
        values = new int[size];

        boolean[] hasValue = new boolean[size];
        int i = 0;
        for (String p : params) {
            int nameEnd = quotedNameEnd(p);
            if (nameEnd == p.length()) {
                names[i] = ClickHouseUtils.unescape(p);
            } else {
                int index = nameEnd > 0 ? nameEnd : p.lastIndexOf('=');
                if (index > 0 && p.charAt(index) == '=') {
                    names[i] = ClickHouseUtils.unescape(p.substring(0, index));
                    values[i] = Integer.parseInt(p.substring(index + 1));
                    hasValue[i] = true;
                } else {
                    throw new IllegalArgumentException("Invalid enum entry: " + p);
                }
            }
            i++;
        }

        // The server gives an entry without a value the number base + position. The base is the value of the
        // first entry, or 1 when the first entry has no value. All entries after the first one need a value, or
        // none of them.
        int valuesAfterFirst = 0;
        for (i = 1; i < size; i++) {
            if (hasValue[i]) {
                valuesAfterFirst++;
            }
        }
        if (valuesAfterFirst != 0 && valuesAfterFirst != size - 1) {
            throw new IllegalArgumentException(
                    "Either all or none of the enum entries after the first one must have a value, got " + params);
        }
        int base = size > 0 && hasValue[0] ? values[0] : 1;
        for (i = 0; i < size; i++) {
            if (!hasValue[i]) {
                values[i] = base + i;
            }
        }
    }

    private static int quotedNameEnd(String entry) {
        int len = entry.length();
        if (len == 0 || !ClickHouseUtils.isQuote(entry.charAt(0))) {
            return -1;
        }

        char quote = entry.charAt(0);
        for (int i = 1; i < len; i++) {
            char ch = entry.charAt(i);
            if (ch == '\\') {
                i++;
            } else if (ch == quote) {
                if (i + 1 < len && entry.charAt(i + 1) == quote) {
                    i++;
                } else {
                    return i + 1;
                }
            }
        }
        return -1;
    }

    public ClickHouseEnum(String[] names, int[] values) {
        if (names == null || values == null) {
            throw new IllegalArgumentException("Non-null names and values are required");
        } else if (names.length != values.length) {
            throw new IllegalArgumentException("Names and values should have same length");
        }

        this.size = names.length;
        this.names = names;
        this.values = values;
    }

    public String validate(String name) {
        for (int i = 0; i < size; i++) {
            if (names[i].equals(name)) {
                return name;
            }
        }

        throw new IllegalArgumentException("Unknown enum name: " + name);
    }

    public int validate(int value) {
        for (int i = 0; i < size; i++) {
            if (values[i] == value) {
                return value;
            }
        }

        throw new IllegalArgumentException("Unknown enum value: " + value);
    }

    public String name(int value) {
        for (int i = 0; i < size; i++) {
            if (values[i] == value) {
                return names[i];
            }
        }

        throw new IllegalArgumentException("Unknown enum value: " + value);
    }

    public String nameNullable(int value) {
        for (int i = 0; i < size; i++) {
            if (values[i] == value) {
                return names[i];
            }
        }

        return null;
    }

    public int value(String name) {
        for (int i = 0; i < size; i++) {
            if (names[i].equals(name)) {
                return values[i];
            }
        }

        throw new IllegalArgumentException("Unknown enum name: " + name);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + size;
        result = prime * result + Arrays.hashCode(names);
        result = prime * result + Arrays.hashCode(values);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ClickHouseEnum other = (ClickHouseEnum) obj;
        return size == other.size && Arrays.equals(names, other.names) && Arrays.equals(values, other.values);
    }

    public String toSqlException() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < size; i++) {
            builder.append('\'').append(ClickHouseUtils.escape(names[i], '\'')).append('\'').append('=')
                    .append(values[i]).append(',');
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    public int size() {
        return size;
    }

    public String[] getNames() {
        return names;
    }

    public int[] getValues() {
        return values;
    }
}
