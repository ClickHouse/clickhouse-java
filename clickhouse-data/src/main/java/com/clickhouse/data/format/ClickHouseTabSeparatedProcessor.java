package com.clickhouse.data.format;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.clickhouse.config.ClickHouseRenameMethod;
import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseDeserializer;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.format.tsv.ByteFragment;

@Deprecated
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
    private final TextDataProcessor.TextSerDe text;

    private ByteFragment currentRow;

    @Override
    protected void readAndFill(ClickHouseRecord r) throws IOException {
        ClickHouseByteBuffer buf = input.readCustom(text::readRecord);
        if (buf.isEmpty() && input.available() < 1) {
            throw new EOFException();
        }
        currentRow = new ByteFragment(buf.array(), buf.position(),
                buf.lastByte() == text.getRecordSeparator() ? buf.length() - 1 : buf.length());

        int len = serde.columns.length;
        int index = readPosition;
        ByteFragment[] currentCols;
        if (len > 1 && text.hasValueSeparator()) {
            currentCols = currentRow.split(text.getValueSeparator());
        } else {
            currentCols = new ByteFragment[] { currentRow };
        }
        for (int i = index; i < len; i++) {
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

        ClickHouseByteBuffer buf = input.readCustom(text::readRecord);
        if (buf.isEmpty()) {
            input.close();
            // no result returned
            return Collections.emptyList();
        }
        ByteFragment headerFragment = new ByteFragment(buf.array(), buf.position(),
                buf.lastByte() == text.getRecordSeparator() ? buf.length() - 1 : buf.length());
        String header = headerFragment.asString(true);
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            input.close();
            throw new IllegalArgumentException("ClickHouse error: " + header);
        }
        String[] cols = toStringArray(headerFragment, text.getValueSeparator());
        String[] types = null;
        if (ClickHouseFormat.TSVWithNamesAndTypes == format
                || ClickHouseFormat.TabSeparatedWithNamesAndTypes == format) {
            buf = input.readCustom(text::readRecord);
            if (buf.isEmpty()) {
                input.close();
                throw new IllegalArgumentException("ClickHouse response without column types");
            }
            ByteFragment typesFragment = new ByteFragment(buf.array(), buf.position(),
                    buf.lastByte() == text.getRecordSeparator() ? buf.length() - 1 : buf.length());
            types = toStringArray(typesFragment, text.getValueSeparator());
        }

        ClickHouseRenameMethod m = config.getColumnRenameMethod();
        List<ClickHouseColumn> list = new ArrayList<>(cols.length);
        for (int i = 0; i < cols.length; i++) {
            list.add(ClickHouseColumn.of(m.rename(cols[i]), types == null ? "Nullable(String)" : types[i]));
        }

        return list;
    }

    public ClickHouseTabSeparatedProcessor(ClickHouseDataConfig config, ClickHouseInputStream input,
            ClickHouseOutputStream output, List<ClickHouseColumn> columns, Map<String, Serializable> settings)
            throws IOException {
        super(config, input, output, columns, settings);

        text = TextDataProcessor.getTextSerDe(config.getFormat());
    }

    @Override
    public void write(ClickHouseValue value) throws IOException {
        if (output == null) {
            throw new IllegalStateException("No output stream available to write");
        }
        DefaultSerDe s = getInitializedSerDe();
        int len = s.columns.length;
        int pos = writePosition;
        if (len == 0 || pos >= len) {
            throw new IllegalStateException(
                    ClickHouseUtils.format("No column to write(total=%d, writePosition=%d)", len, pos));
        }
        if (value == null) {
            value = config.isReuseValueWrapper() ? s.templates[pos] : s.templates[pos].copy();
        }
        text.serialize(value, output);
        if (++pos >= len) {
            output.writeByte(text.getRecordSeparator());
            writePosition = 0;
        } else {
            output.writeByte(text.getValueSeparator());
            writePosition = pos;
        }
    }

    @Override
    public ClickHouseDeserializer getDeserializer(ClickHouseDataConfig config, ClickHouseColumn column) {
        ClickHouseDataType dt = column.getDataType();
        return dt == ClickHouseDataType.FixedString || (config.isUseBinaryString() && dt == ClickHouseDataType.String)
                ? text::deserializeBinary
                : text;
    }

    @Override
    public ClickHouseSerializer getSerializer(ClickHouseDataConfig config, ClickHouseColumn column) {
        ClickHouseDataType dt = column.getDataType();
        return dt == ClickHouseDataType.FixedString || (config.isUseBinaryString() && dt == ClickHouseDataType.String)
                ? text::serializeBinary
                : text;
    }
}
