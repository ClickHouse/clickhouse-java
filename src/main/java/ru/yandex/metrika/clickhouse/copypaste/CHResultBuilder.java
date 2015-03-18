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
    private List<List<Object>> rows = new ArrayList<List<Object>>();

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
        if (names.size() != columnsNum) throw new IllegalArgumentException("size mismatch");
        this.names = names;
        return this;
    }

    public CHResultBuilder types(List<String> types) {
        if (types.size() != columnsNum) throw new IllegalArgumentException("size mismatch");
        this.types = types;
        return this;
    }

    public CHResultBuilder addRow(List<Object> row) {
        if (row.size() != columnsNum) throw new IllegalArgumentException("size mismatch");
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
            for (List<Object> row : rows) {
                appendRow(row, baos);
            }

            byte[] bytes = baos.toByteArray();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

            return new CHResultSet(inputStream, 1024);
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
            ByteFragment.escape(o.toString().getBytes(), baos);
        }
    }

}
