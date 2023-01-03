package com.clickhouse.client.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.clickhouse.client.ClickHouseByteBuffer;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataProcessor;
import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseDeserializer;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseSerializer;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseRenameMethod;
import com.clickhouse.client.data.tsv.ByteFragment;

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

    // initialize in readColumns()
    private TextDataProcessor.TextSerDe serde;
    private ByteFragment currentRow;

    protected TextDataProcessor.TextSerDe getTextSerDe() {
        if (serde == null) {
            serde = TextDataProcessor.getTextSerDe(config.getFormat());
        }
        return serde;
    }

    @Override
    protected ClickHouseRecord createRecord() {
        return new ClickHouseSimpleRecord(getColumns(), templates);
    }

    @Override
    protected void readAndFill(ClickHouseRecord r) throws IOException {
        TextDataProcessor.TextSerDe ds = getTextSerDe();
        ClickHouseByteBuffer buf = input.readCustom(ds::readRecord);
        if (buf.isEmpty() && input.available() < 1) {
            throw new EOFException();
        }
        currentRow = new ByteFragment(buf.array(), buf.position(),
                buf.lastByte() == ds.getRecordSeparator() ? buf.length() - 1 : buf.length());

        int index = readPosition;
        ByteFragment[] currentCols;
        if (columns.length > 1 && ds.hasValueSeparator()) {
            currentCols = currentRow.split(ds.getValueSeparator());
        } else {
            currentCols = new ByteFragment[] { currentRow };
        }
        for (int i = index, len = columns.length; i < len; i++) {
            r.getValue(i).update(currentCols[i - index].asString(true));
            readPosition = i;
        }
        readPosition = 0;
    }

    @Override
    protected void readAndFill(ClickHouseValue value) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<ClickHouseColumn> readColumns() throws IOException {
        if (input.available() < 1) {
            input.close();
            return Collections.emptyList();
        }
        ClickHouseFormat format = config.getFormat();
        if (!format.hasHeader()) {
            return DEFAULT_COLUMNS;
        }

        TextDataProcessor.TextSerDe ds = getTextSerDe();
        ClickHouseByteBuffer buf = input.readCustom(ds::readRecord);
        if (buf.isEmpty()) {
            input.close();
            // no result returned
            return Collections.emptyList();
        }
        ByteFragment headerFragment = new ByteFragment(buf.array(), buf.position(),
                buf.lastByte() == ds.getRecordSeparator() ? buf.length() - 1 : buf.length());
        String header = headerFragment.asString(true);
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            input.close();
            throw new IllegalArgumentException("ClickHouse error: " + header);
        }
        String[] cols = toStringArray(headerFragment, ds.getValueSeparator());
        String[] types = null;
        if (ClickHouseFormat.TSVWithNamesAndTypes == format
                || ClickHouseFormat.TabSeparatedWithNamesAndTypes == format) {
            buf = input.readCustom(ds::readRecord);
            if (buf.isEmpty()) {
                input.close();
                throw new IllegalArgumentException("ClickHouse response without column types");
            }
            ByteFragment typesFragment = new ByteFragment(buf.array(), buf.position(),
                    buf.lastByte() == ds.getRecordSeparator() ? buf.length() - 1 : buf.length());
            types = toStringArray(typesFragment, ds.getValueSeparator());
        }

        ClickHouseRenameMethod m = config.getOption(ClickHouseClientOption.RENAME_RESPONSE_COLUMN,
                ClickHouseRenameMethod.class);
        List<ClickHouseColumn> list = new ArrayList<>(cols.length);
        for (int i = 0; i < cols.length; i++) {
            list.add(ClickHouseColumn.of(m.rename(cols[i]), types == null ? "Nullable(String)" : types[i]));
        }

        return list;
    }

    public ClickHouseTabSeparatedProcessor(ClickHouseConfig config, ClickHouseInputStream input,
            ClickHouseOutputStream output, List<ClickHouseColumn> columns, Map<String, Serializable> settings)
            throws IOException {
        super(config, input, output, columns, settings);
    }

    @Override
    public void write(ClickHouseValue value) throws IOException {
        serde.serialize(value, output);
        if (++writePosition < columns.length) {
            output.writeByte(serde.getValueSeparator());
        } else {
            output.writeByte(serde.getRecordSeparator());
            writePosition = 0;
        }
    }

    @Override
    public ClickHouseDeserializer getDeserializer(ClickHouseConfig config, ClickHouseColumn column) {
        ClickHouseDataType dt = column.getDataType();
        return dt == ClickHouseDataType.FixedString || (config.isUseBinaryString() && dt == ClickHouseDataType.String)
                ? getTextSerDe()::deserializeBinary
                : getTextSerDe();
    }

    @Override
    public ClickHouseSerializer getSerializer(ClickHouseConfig config, ClickHouseColumn column) {
        ClickHouseDataType dt = column.getDataType();
        return dt == ClickHouseDataType.FixedString || (config.isUseBinaryString() && dt == ClickHouseDataType.String)
                ? getTextSerDe()::serializeBinary
                : getTextSerDe();
    }
}
