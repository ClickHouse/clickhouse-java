package com.clickhouse.client.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import com.clickhouse.client.ClickHouseByteBuffer;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataProcessor;
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
    private static final Map<ClickHouseFormat, MappedFunctions> cachedFuncs = new EnumMap<>(ClickHouseFormat.class);

    static final class TextHandler {
        static final byte[] NULL = "\\N".getBytes(StandardCharsets.US_ASCII);

        private final byte colDelimiter;
        private final byte rowDelimiter;
        private final byte escapeByte;

        private byte prev;

        TextHandler(char rowDelimiter) {
            this.colDelimiter = 0x0;
            this.rowDelimiter = (byte) rowDelimiter;
            this.escapeByte = 0x0;

            this.prev = 0x0;
        }

        TextHandler(char colDelimiter, char rowDelimiter, char escapeByte) {
            this.colDelimiter = (byte) colDelimiter;
            this.rowDelimiter = (byte) rowDelimiter;
            this.escapeByte = (byte) escapeByte;

            this.prev = 0x0;
        }

        private int read(byte[] bytes, int position, int limit) {
            int offset = 0;
            for (int i = position; i < limit; i++) {
                byte b = bytes[i];
                offset++;
                if (b == rowDelimiter) {
                    return offset;
                }
            }

            return -1;
        }

        int readLine(byte[] bytes, int position, int limit) {
            if (escapeByte == 0x0) {
                return read(bytes, position, limit);
            }

            int offset = 0;
            for (int i = position; i < limit; i++) {
                byte b = bytes[i];
                offset++;
                if (prev == escapeByte) {
                    prev = b == escapeByte ? 0x0 : b;
                } else {
                    prev = b;
                    if (b == rowDelimiter) {
                        return offset;
                    }
                }
            }

            return -1;
        }

        int readColumn(byte[] bytes, int position, int limit) {
            if (colDelimiter == 0x0) {
                return readLine(bytes, position, limit);
            }

            int offset = 0;
            for (int i = position; i < limit; i++) {
                byte b = bytes[i];
                offset++;
                if (prev == escapeByte) {
                    prev = b == escapeByte ? 0x0 : b;
                } else {
                    prev = b;
                    if (b == colDelimiter || b == rowDelimiter) {
                        return offset;
                    }
                }
            }

            return -1;
        }

        int writeColumn(byte[] bytes, int position, int limit) {
            if (colDelimiter == 0x0) {
                return writeLine(bytes, position, limit);
            } else {
                bytes[position] = colDelimiter;
                return 1;
            }
        }

        int writeLine(byte[] bytes, int position, int limit) {
            if (rowDelimiter == 0x0) {
                return 0;
            } else {
                bytes[position] = rowDelimiter;
                return 1;
            }
        }
    }

    public static final class MappedFunctions
            implements ClickHouseDeserializer<ClickHouseValue>, ClickHouseSerializer<ClickHouseValue> {
        private final TextHandler textHandler;

        private MappedFunctions(ClickHouseFormat format) {
            this.textHandler = getTextHandler(format);
        }

        /**
         * Deserializes data read from input stream.
         *
         * @param ref    wrapper object can be reused, could be null(always return new
         *               wrapper object)
         * @param config non-null configuration
         * @param column non-null type information
         * @param input  non-null input stream
         * @return deserialized value which might be the same instance as {@code ref}
         * @throws IOException when failed to read data from input stream
         */
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseConfig config, ClickHouseColumn column,
                ClickHouseInputStream input) throws IOException {
            if (ref == null) {
                ref = ClickHouseStringValue.ofNull();
            }
            return ref.update(
                    input.readCustom(column.isLastColumn() && column.isFirstColumn() ? textHandler::readLine
                            : textHandler::readColumn).asUnicodeString());
        }

        /**
         * Writes serialized value to output stream.
         *
         * @param value  non-null value to be serialized
         * @param config non-null configuration
         * @param column non-null type information
         * @param output non-null output stream
         * @throws IOException when failed to write data to output stream
         */
        public void serialize(ClickHouseValue value, ClickHouseConfig config, ClickHouseColumn column,
                ClickHouseOutputStream output) throws IOException {
            output.writeBytes(value.isNullOrEmpty() ? TextHandler.NULL : value.asBinary());
            output.writeCustom(column.isLastColumn() ? textHandler::writeLine : textHandler::writeColumn);
        }
    }

    private static TextHandler getTextHandler(ClickHouseFormat format) {
        final TextHandler textHandler;
        switch (format) {
            case CSV:
            case CSVWithNames:
                textHandler = new TextHandler(',', '\t', '\\');
                break;
            case TSV:
            case TSVRaw:
            case TSVWithNames:
            case TSVWithNamesAndTypes:
            case TabSeparated:
            case TabSeparatedRaw:
            case TabSeparatedWithNames:
            case TabSeparatedWithNamesAndTypes:
                textHandler = new TextHandler('\t', '\n', '\\');
                break;
            default:
                textHandler = new TextHandler('\n');
                break;
        }

        return textHandler;
    }

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

    public static MappedFunctions getMappedFunctions(ClickHouseFormat format) {
        return cachedFuncs.computeIfAbsent(format, MappedFunctions::new);
    }

    // initialize in readColumns()
    private TextHandler textHandler;
    private ByteFragment currentRow;

    protected TextHandler getTextHandler() {
        if (textHandler == null) {
            textHandler = getTextHandler(config.getFormat());
        }
        return textHandler;
    }

    @Override
    protected ClickHouseRecord createRecord() {
        return new ClickHouseSimpleRecord(getColumns(), templates);
    }

    @Override
    protected void readAndFill(ClickHouseRecord r) throws IOException {
        ClickHouseByteBuffer buf = input.readCustom(getTextHandler()::readLine);
        if (buf.isEmpty() && input.available() < 1) {
            throw new EOFException();
        }
        currentRow = new ByteFragment(buf.array(), buf.position(),
                buf.lastByte() == getTextHandler().rowDelimiter ? buf.length() - 1 : buf.length());

        int index = readPosition;
        byte delimiter = getTextHandler().colDelimiter;
        ByteFragment[] currentCols;
        if (columns.length > 1 && delimiter != (byte) 0) {
            currentCols = currentRow.split(delimiter);
        } else {
            currentCols = new ByteFragment[] { currentRow };
        }
        for (; readPosition < columns.length; readPosition++) {
            r.getValue(readPosition).update(currentCols[readPosition - index].asString(true));
        }
    }

    @Override
    protected void readAndFill(ClickHouseValue value, ClickHouseColumn column) throws IOException {
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

        ClickHouseByteBuffer buf = input.readCustom(getTextHandler()::readLine);
        if (buf.isEmpty()) {
            input.close();
            // no result returned
            return Collections.emptyList();
        }
        ByteFragment headerFragment = new ByteFragment(buf.array(), buf.position(),
                buf.lastByte() == getTextHandler().rowDelimiter ? buf.length() - 1 : buf.length());
        String header = headerFragment.asString(true);
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            input.close();
            throw new IllegalArgumentException("ClickHouse error: " + header);
        }
        String[] cols = toStringArray(headerFragment, getTextHandler().colDelimiter);
        String[] types = null;
        if (ClickHouseFormat.TSVWithNamesAndTypes == format
                || ClickHouseFormat.TabSeparatedWithNamesAndTypes == format) {
            buf = input.readCustom(getTextHandler()::readLine);
            if (buf.isEmpty()) {
                input.close();
                throw new IllegalArgumentException("ClickHouse response without column types");
            }
            ByteFragment typesFragment = new ByteFragment(buf.array(), buf.position(),
                    buf.lastByte() == getTextHandler().rowDelimiter ? buf.length() - 1 : buf.length());
            types = toStringArray(typesFragment, getTextHandler().colDelimiter);
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
    public void write(ClickHouseValue value, ClickHouseColumn column) throws IOException {
        if (output == null || column == null) {
            throw new IllegalArgumentException("Cannot write any value when output stream or column is null");
        }
        output.writeBytes(value.isNullOrEmpty() ? TextHandler.NULL : value.asBinary());
        output.writeCustom(column.isLastColumn() ? getTextHandler()::writeLine : getTextHandler()::writeColumn);
    }
}
