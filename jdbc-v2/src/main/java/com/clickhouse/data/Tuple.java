package com.clickhouse.data;

import java.util.Arrays;

public class Tuple {
    private final Object[] values;
    private volatile String output;
    public Tuple(Object... values) {
        this.values = values;
    }

    public Object[] getValues() {
        return values;
    }

    public Object getValue(int index) {
        return values[index];
    }

    public int size() {
        return values.length;
    }
    private String buildOutput() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(values[i]);
        }
        sb.append(")");
        return sb.toString();
    }
    @Override
    public String toString() {
        if (output == null) {
            output = buildOutput();
        }
        return output;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Tuple) {
            Tuple other = (Tuple) obj;
            return Arrays.equals(values, other.values);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }
}
