package ru.yandex.clickhouse.response;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * For building ClickHouseResultSet by hands
 */
public class ClickHouseResultBuilder {

    private final int columnsNum;
    private List<String> names;
    private List<String> types;
    private List<List<?>> rows = new ArrayList<List<?>>();

    public static ClickHouseResultBuilder builder(int columnsNum) {
        return new ClickHouseResultBuilder(columnsNum);
    }

    private ClickHouseResultBuilder(int columnsNum) {
        this.columnsNum = columnsNum;
    }

    public ClickHouseResultBuilder names(String... names) {
        return names(Arrays.asList(names));
    }

    public ClickHouseResultBuilder types(String... types) {
        return types(Arrays.asList(types));
    }

    public ClickHouseResultBuilder addRow(Object... row) {
        return addRow(Arrays.asList(row));
    }

    public ClickHouseResultBuilder names(List<String> names) {
        if (names.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + names.size());
        this.names = names;
        return this;
    }

    public ClickHouseResultBuilder types(List<String> types) {
        if (types.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + types.size());
        this.types = types;
        return this;
    }

    public ClickHouseResultBuilder addRow(List<?> row) {
        if (row.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + row.size());
        rows.add(row);
        return this;
    }

    public ClickHouseResultSet build() {
        try {
            if (names == null) throw new IllegalStateException("names == null");
            if (types == null) throw new IllegalStateException("types == null");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            appendRow(names, baos);
            appendRow(types, baos);
            for (List<?> row : rows) {
                appendRow(row, baos);
            }

            byte[] bytes = baos.toByteArray();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

            return new ClickHouseResultSet(inputStream, 1024, "system", "unknown", null);
        } catch (IOException e) {
            throw new RuntimeException("Never happens", e);
        }
    }

    private void appendRow(List<?> row, ByteArrayOutputStream baos) throws IOException {
        for (int i = 0; i < row.size(); i++) {
            if (i != 0) baos.write('\t');
            appendObject(row.get(i), baos);
        }
        baos.write('\n');
    }

    private void appendObject(Object o, ByteArrayOutputStream baos) throws IOException {
        if (o == null) {
            baos.write('\\');
            baos.write('N');
        } else {
            String value;
            if (o instanceof Boolean) {
                if ((Boolean) o) {
                    value = "1";
                } else {
                    value = "0";
                }
            } else {
                value = o.toString();
            }
            ByteFragment.escape(value.getBytes(), baos);
        }
    }

}
