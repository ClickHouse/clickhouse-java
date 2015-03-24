package ru.yandex.metrika.clickhouse.copypaste;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Для ручного собирания CHResultSet
 * Created by jkee on 17.03.15.
 */
public class CHResultBuilder {

    private final int columnsNum;
    private List<String> names;
    private List<String> types;
    private List<List<?>> rows = new ArrayList<List<?>>();

    public static CHResultBuilder builder(int columnsNum) {
        return new CHResultBuilder(columnsNum);
    }

    private CHResultBuilder(int columnsNum) {
        this.columnsNum = columnsNum;
    }

    public CHResultBuilder names(String... names) {
        return names(Arrays.asList(names));
    }

    public CHResultBuilder types(String... types) {
        return types(Arrays.asList(types));
    }

    public CHResultBuilder addRow(Object... row) {
        return addRow(Arrays.asList(row));
    }

    public CHResultBuilder names(List<String> names) {
        if (names.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + names.size());
        this.names = names;
        return this;
    }

    public CHResultBuilder types(List<String> types) {
        if (types.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + types.size());
        this.types = types;
        return this;
    }

    public CHResultBuilder addRow(List<?> row) {
        if (row.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + row.size());
        rows.add(row);
        return this;
    }

    public CHResultSet build() {
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

            return new CHResultSet(inputStream, 1024, "system", "unknown");
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
