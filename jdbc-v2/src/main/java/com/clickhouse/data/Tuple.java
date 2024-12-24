package com.clickhouse.data;

public class Tuple {
    private final Object[] values;

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

    @Override
    public String toString() {
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
}
