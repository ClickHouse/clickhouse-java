package com.clickhouse.data;

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
}
