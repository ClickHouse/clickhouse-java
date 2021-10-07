package com.clickhouse.client.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataProcessor;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.data.tsv.ByteFragment;
import com.clickhouse.client.data.tsv.StreamSplitter;

public class ClickHouseTabSeparatedProcessor extends ClickHouseDataProcessor {
    private static String[] toStringArray(ByteFragment headerFragment) {
        ByteFragment[] split = headerFragment.split((byte) 0x09);
        String[] array = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            array[i] = split[i].asString(true);
        }
        return array;
    }

    private class Records implements Iterator<ClickHouseRecord> {
        private ByteFragment currentRow;

        Records() {
            if (!columns.isEmpty()) {
                readNextRow();
            }
        }

        void readNextRow() {
            try {
                currentRow = splitter.next();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return currentRow != null;
        }

        @Override
        public ClickHouseRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more record");
            }

            ByteFragment[] currentCols = currentRow.split((byte) 0x09);
            readNextRow();

            return new ClickHouseRecord() {
                @Override
                public int size() {
                    return currentCols.length;
                }

                @Override
                public ClickHouseValue getValue(int index) {
                    return ClickHouseStringValue.of(null, currentCols[index].asString(true));
                }

                @Override
                public ClickHouseValue getValue(String columnName) throws IOException {
                    int index = 0;
                    for (ClickHouseColumn c : columns) {
                        if (c.getColumnName().equals(columnName)) {
                            getValue(index);
                        }
                        index++;
                    }

                    throw new IllegalArgumentException("Not able to find a column named: " + columnName);
                }
            };
        }
    }

    private StreamSplitter splitter;

    @Override
    public List<ClickHouseColumn> readColumns() throws IOException {
        if (input == null) {
            return Collections.emptyList();
        } else if (!config.getFormat().hasHeader()) {
            return DEFAULT_COLUMNS;
        }

        this.splitter = new StreamSplitter(input, (byte) 0x0A, config.getMaxBufferSize());

        ByteFragment headerFragment = this.splitter.next();
        if (headerFragment == null) {
            throw new IllegalArgumentException("ClickHouse response without column names");
        }
        String header = headerFragment.asString(true);
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            input.close();
            throw new IllegalArgumentException("ClickHouse error: " + header);
        }
        String[] cols = toStringArray(headerFragment);
        String[] types = null;
        if (ClickHouseFormat.TabSeparatedWithNamesAndTypes == config.getFormat()) {
            ByteFragment typesFragment = splitter.next();
            if (typesFragment == null) {
                throw new IllegalArgumentException("ClickHouse response without column types");
            }

            types = toStringArray(typesFragment);
        }
        List<ClickHouseColumn> list = new ArrayList<>(cols.length);

        for (int i = 0; i < cols.length; i++) {
            list.add(ClickHouseColumn.of(cols[i], types == null ? "Nullable(String)" : types[i]));
        }

        return list;
    }

    public ClickHouseTabSeparatedProcessor(ClickHouseConfig config, InputStream input, OutputStream output,
            List<ClickHouseColumn> columns, Map<String, Object> settings) throws IOException {
        super(config, input, output, columns, settings);

        if (this.splitter == null && input != null) {
            this.splitter = new StreamSplitter(input, (byte) 0x0A, config.getMaxBufferSize());
        }
    }

    @Override
    public Iterable<ClickHouseRecord> records() {
        return new Iterable<ClickHouseRecord>() {
            @Override
            public Iterator<ClickHouseRecord> iterator() {
                return new Records();
            }
        };
    }
}
