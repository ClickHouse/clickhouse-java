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
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.data.tsv.ByteFragment;
import com.clickhouse.client.data.tsv.StreamSplitter;

public class ClickHouseTabSeparatedProcessor extends ClickHouseDataProcessor {
    private static String[] toStringArray(ByteFragment headerFragment, byte delimitter) {
        if (delimitter == (byte) 0) {
            return new String[] { headerFragment.asString(true) };
        }

        ByteFragment[] split = headerFragment.split(delimitter);
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

            ByteFragment[] currentCols = colDelimitter != (byte) 0 ? currentRow.split(colDelimitter)
                    : new ByteFragment[] { currentRow };
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
                public ClickHouseValue getValue(String name) {
                    int index = 0;
                    for (ClickHouseColumn c : columns) {
                        if (c.getColumnName().equalsIgnoreCase(name)) {
                            return getValue(index);
                        }
                        index++;
                    }

                    throw new IllegalArgumentException(ClickHouseUtils.format("Unable to find column [%s]", name));
                }
            };
        }
    }

    private final byte rowDelimitter = (byte) 0x0A;

    // initialize in readColumns()
    private byte colDelimitter;
    private StreamSplitter splitter;

    @Override
    public List<ClickHouseColumn> readColumns() throws IOException {
        if (input == null) {
            return Collections.emptyList();
        }

        ClickHouseFormat format = config.getFormat();
        if (!format.hasHeader()) {
            return DEFAULT_COLUMNS;
        }

        switch (config.getFormat()) {
        case TSVWithNames:
        case TSVWithNamesAndTypes:
        case TabSeparatedWithNames:
        case TabSeparatedWithNamesAndTypes:
            colDelimitter = (byte) 0x09;
            break;
        default:
            colDelimitter = (byte) 0;
            break;
        }

        this.splitter = new StreamSplitter(input, rowDelimitter, config.getMaxBufferSize());

        ByteFragment headerFragment = this.splitter.next();
        if (headerFragment == null) {
            throw new IllegalArgumentException("ClickHouse response without column names");
        }
        String header = headerFragment.asString(true);
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            input.close();
            throw new IllegalArgumentException("ClickHouse error: " + header);
        }
        String[] cols = toStringArray(headerFragment, colDelimitter);
        String[] types = null;
        if (ClickHouseFormat.TSVWithNamesAndTypes == format
                || ClickHouseFormat.TabSeparatedWithNamesAndTypes == format) {
            ByteFragment typesFragment = splitter.next();
            if (typesFragment == null) {
                throw new IllegalArgumentException("ClickHouse response without column types");
            }

            types = toStringArray(typesFragment, colDelimitter);
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
            this.splitter = new StreamSplitter(input, rowDelimitter, config.getMaxBufferSize());
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
