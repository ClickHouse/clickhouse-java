package com.clickhouse.client.testdata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;

/**
 * Rows of generated values for a set of columns, available both as Java objects and as SQL.
 *
 * <p>Every row starts with the {@value #ID_COLUMN} column ({@code Int32}, Java {@code Integer}) holding the
 * row number, followed by the configured columns in order. The number of rows equals the largest number of
 * values generated for a column; columns with fewer values repeat them cyclically.
 *
 * <p>Values are generated from {@link #DEFAULT_SEED} unless another seed is set, so a failing data set can be
 * reproduced with {@code -Dclickhouse.test.seed=<seed>}. Values like {@code byte[]} and {@code double[]} have
 * identity equality, so rows should be compared with deep equality.
 */
public final class TestDataSet {

    public static final String ID_COLUMN = "id";
    public static final long DEFAULT_SEED = Long.getLong("clickhouse.test.seed", 20261005L);

    private final long seed;
    private final Map<String, DataTypeGenerator> columns;
    private final List<Object[]> rows;

    private TestDataSet(long seed, Map<String, DataTypeGenerator> columns) {
        this.seed = seed;
        this.columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));

        Random random = new Random(seed);
        List<List<Object>> values = new ArrayList<>(columns.size());
        for (DataTypeGenerator generator : columns.values()) {
            values.add(generator.generate(random));
        }
        List<Object[]> rows = new ArrayList<>();
        for (List<Object> row : DataTypeGenerators.zip(values)) {
            Object[] fullRow = new Object[row.size() + 1];
            fullRow[0] = rows.size();
            for (int i = 0; i < row.size(); i++) {
                fullRow[i + 1] = row.get(i);
            }
            rows.add(fullRow);
        }
        this.rows = Collections.unmodifiableList(rows);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * @return data set with a single column named {@code value}
     */
    public static TestDataSet of(DataTypeGenerator generator) {
        return builder().column("value", generator).build();
    }

    public long getSeed() {
        return seed;
    }

    /**
     * @return column names and their generators, without the {@value #ID_COLUMN} column
     */
    public Map<String, DataTypeGenerator> getColumns() {
        return columns;
    }

    /**
     * @return rows of Java values, each starting with the row number
     */
    public List<Object[]> getRows() {
        return rows;
    }

    /**
     * @return rows in the TestNG {@code @DataProvider} format
     */
    public Object[][] toDataProvider() {
        Object[][] data = new Object[rows.size()][];
        for (int i = 0; i < data.length; i++) {
            data[i] = rows.get(i).clone();
        }
        return data;
    }

    /**
     * @return server settings required by the column types, to be passed with every query
     */
    public Map<String, String> getRequiredSettings() {
        Map<String, String> settings = new LinkedHashMap<>();
        for (DataTypeGenerator generator : columns.values()) {
            settings.putAll(generator.getRequiredSettings());
        }
        return settings;
    }

    public String getCreateTableSql(String table) {
        StringJoiner definition = new StringJoiner(", ", "CREATE TABLE " + table + " (",
                ") ENGINE = MergeTree ORDER BY " + ID_COLUMN);
        definition.add(ID_COLUMN + " Int32");
        for (Map.Entry<String, DataTypeGenerator> column : columns.entrySet()) {
            if (!column.getValue().isStorable()) {
                throw new IllegalStateException("Column " + column.getKey() + " of type "
                        + column.getValue().getType() + " cannot be stored in a table");
            }
            definition.add(DataTypeGenerators.quoteIdentifier(column.getKey()) + " " + column.getValue().getType());
        }
        return definition.toString();
    }

    public String getInsertSql(String table) {
        StringJoiner names = new StringJoiner(", ", "INSERT INTO " + table + " (", ") VALUES ");
        names.add(ID_COLUMN);
        for (String name : columns.keySet()) {
            names.add(DataTypeGenerators.quoteIdentifier(name));
        }
        return names + getValuesClause();
    }

    /**
     * @return rows as SQL tuples separated by commas, for example {@code (0, 'a'),\n(1, 'b')}
     */
    public String getValuesClause() {
        List<DataTypeGenerator> generators = new ArrayList<>(columns.values());
        StringJoiner values = new StringJoiner(",\n");
        for (Object[] row : rows) {
            StringJoiner tuple = new StringJoiner(", ", "(", ")");
            tuple.add(row[0].toString());
            for (int i = 0; i < generators.size(); i++) {
                tuple.add(generators.get(i).toSqlLiteral(row[i + 1]));
            }
            values.add(tuple.toString());
        }
        return values.toString();
    }

    /**
     * Rows as ClickHouse writes them in the {@code TabSeparated} format, for example the result of
     * {@code SELECT * FROM table ORDER BY id FORMAT TSV}. Each char is one byte, so the text must be written with
     * {@link java.nio.charset.StandardCharsets#ISO_8859_1} to get the UTF-8 encoded content.
     *
     * @return rows separated and terminated by {@code \n}, values separated by {@code \t}
     */
    public String getTsv() {
        List<DataTypeGenerator> generators = new ArrayList<>(columns.values());
        StringBuilder tsv = new StringBuilder();
        for (Object[] row : rows) {
            tsv.append(row[0]);
            for (int i = 0; i < generators.size(); i++) {
                tsv.append('\t').append(generators.get(i).toTsv(row[i + 1]));
            }
            tsv.append('\n');
        }
        return tsv.toString();
    }

    @Override
    public String toString() {
        StringJoiner description = new StringJoiner(", ", "TestDataSet(", ")");
        for (Map.Entry<String, DataTypeGenerator> column : columns.entrySet()) {
            description.add(column.getKey() + " " + column.getValue().getType());
        }
        return description + " rows=" + rows.size() + " seed=" + seed;
    }

    public static final class Builder {
        private final Map<String, DataTypeGenerator> columns = new LinkedHashMap<>();
        private long seed = DEFAULT_SEED;

        private Builder() {
        }

        public Builder seed(long seed) {
            this.seed = seed;
            return this;
        }

        public Builder column(String name, DataTypeGenerator generator) {
            if (ID_COLUMN.equals(name) || columns.containsKey(name)) {
                throw new IllegalArgumentException("Duplicate column name: " + name);
            }
            columns.put(name, generator);
            return this;
        }

        public TestDataSet build() {
            if (columns.isEmpty()) {
                throw new IllegalStateException("Data set must have at least one column");
            }
            return new TestDataSet(seed, columns);
        }
    }
}
