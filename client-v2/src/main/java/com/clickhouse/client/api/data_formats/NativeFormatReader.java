package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * For the backward compatibility server will not send TZ id in column type. Client should send version to a server
 * to get the correct column type.
 * (see: https://github.com/ClickHouse/ClickHouse/issues/38209)
 */
public class NativeFormatReader extends AbstractBinaryFormatReader {

    private Block currentBlock;

    private int blockRowIndex;

    public NativeFormatReader(InputStream inputStream, QuerySettings settings,
                              BinaryStreamReader.ByteBufferAllocator byteBufferAllocator,
                              Map<ClickHouseDataType, Class<?>> typeHintMapping) {
        super(inputStream, settings, null, byteBufferAllocator, typeHintMapping);
        try {
            readBlock();
        } catch (IOException e) {
            throw new ClientException("Failed to read block", e);
        }
    }

    public NativeFormatReader(InputStream inputStream, QuerySettings settings,
                              BinaryStreamReader.ByteBufferAllocator byteBufferAllocator) {
        this(inputStream, settings, byteBufferAllocator, NO_TYPE_HINT_MAPPING);
    }

    @Override
    public boolean readRecord(Map<String, Object> record) throws IOException {
        if (blockRowIndex >= currentBlock.getnRows()) {
            if (!readBlock()) {
                return false;
            }
        }

        currentBlock.fillRecord(blockRowIndex, record);
        blockRowIndex++;
        return true;
    }

    @Override
    protected boolean readRecord(Object[] record) throws IOException {
        if (blockRowIndex >= currentBlock.getnRows()) {
            if (!readBlock()) {
                return false;
            }
        }

        currentBlock.fillRecord(blockRowIndex, record);
        blockRowIndex++;
        return true;
    }

    private boolean readBlock() throws IOException {
        int nColumns;
        try {
            nColumns = BinaryStreamReader.readVarInt(input);
        } catch (EOFException e) {
            endReached();
            return false;
        }
        int nRows = BinaryStreamReader.readVarInt(input);

        List<String> names = new ArrayList<>(nColumns);
        List<String> types = new ArrayList<>(nColumns);
        currentBlock = new Block(names, types, nRows);
        List<ClickHouseColumn> columns = new ArrayList<>(nColumns);
        for (int i = 0; i < nColumns; i++) {

            ClickHouseColumn column = ClickHouseColumn.of(BinaryStreamReader.readString(input),
                    BinaryStreamReader.readString(input));
            columns.add(column);

            names.add(column.getColumnName());
            types.add(column.getDataType().name());

            List<Object> values;
            if (isNativeDecodableQBit(column)) {
                // Decode the Native bit-plane layout (docs/qbit-encoding.md).
                values = binaryStreamReader.readQBitNative(column, nRows);
            } else if (containsQBit(column)) {
                // Any other QBit shape (non-float element, strided, Nullable/LowCardinality, or nested)
                // uses a Native layout this reader does not decode; fail loudly rather than misread the
                // block and desynchronize the columns that follow (docs/qbit-encoding.md).
                throw new ClientException("Reading column '" + column.getColumnName() + "' ("
                        + column.getOriginalTypeName() + ") from the Native format is not supported: "
                        + "this reader decodes only a plain top-level QBit column with a Float32, Float64 "
                        + "or BFloat16 element type. A QBit whose element type is none of those, or that is "
                        + "strided, wrapped in Nullable/LowCardinality, or nested inside another type "
                        + "(e.g. Array/Tuple/Map), is not decoded. Use a RowBinary format "
                        + "(e.g. RowBinaryWithNamesAndTypes) to read such QBit values");
            } else if (column.isArray()) {
                // Native encodes an Array column as nRows cumulative offsets followed by the
                // flattened elements; each row's element count is the delta between consecutive
                // offsets, not the first offset.
                values = new ArrayList<>(nRows);
                long[] offsets = new long[nRows];
                for (int j = 0; j < nRows; j++) {
                    offsets[j] = binaryStreamReader.readLongLE();
                }
                long prevOffset = 0;
                for (int j = 0; j < nRows; j++) {
                    int len = Math.toIntExact(offsets[j] - prevOffset);
                    values.add(binaryStreamReader.readArrayItem(column.getNestedColumns().get(0), len));
                    prevOffset = offsets[j];
                }
            } else {
                values = new ArrayList<>(nRows);
                for (int j = 0; j < nRows; j++) {
                    Object value = binaryStreamReader.readValue(column);
                    values.add(value);
                }
            }
            currentBlock.add(values);
        }
        TableSchema schema = new TableSchema(columns);

        setSchema(schema);

        blockRowIndex = 0;
        return true;
    }

    /**
     * Returns {@code true} for a plain top-level {@code QBit(Float32|Float64|BFloat16, dimension)} the
     * reader can decode from the Native bit-plane layout — not {@code Nullable}/{@code LowCardinality},
     * not strided, not nested. Other QBit shapes are caught by {@link #containsQBit} and rejected in
     * {@link #readBlock} (see {@code docs/qbit-encoding.md}).
     */
    private static boolean isNativeDecodableQBit(ClickHouseColumn column) {
        if (column.getDataType() != ClickHouseDataType.QBit
                || column.isNullable() || column.isLowCardinality()
                || column.getNestedColumns().isEmpty()) {
            return false;
        }
        // A strided QBit has a third parameter (stride) and a plane count this decoder does not handle.
        if (column.getParameters().size() > 2) {
            return false;
        }
        switch (column.getNestedColumns().get(0).getDataType()) {
            case Float32:
            case Float64:
            case BFloat16:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns {@code true} if {@code column} is or contains a {@code QBit} anywhere in its nested type
     * tree (e.g. {@code Array}/{@code Tuple}/{@code Map(String, QBit(...))}). Used by {@link #readBlock}
     * to reject the QBit shapes {@link #isNativeDecodableQBit} does not decode. {@code Nullable}/
     * {@code LowCardinality} are column flags, so a wrapped {@code QBit} still reports
     * {@code dataType == QBit} here.
     */
    private static boolean containsQBit(ClickHouseColumn column) {
        if (column.getDataType() == ClickHouseDataType.QBit) {
            return true;
        }
        for (ClickHouseColumn nested : column.getNestedColumns()) {
            if (nested != column && containsQBit(nested)) {
                return true;
            }
        }
        return false;
    }

    private static class Block {
        final List<String> names;
        final List<String> types;

        final List<List<Object>> values = new ArrayList<>();
        final int nRows;

        Block(List<String> names, List<String> types, int nRows) {
            this.names = names;
            this.types = types;
            this.nRows = nRows;
        }

        public void add(List<Object> values) {
            this.values.add(values);
        }

        public int getnRows() {
            return nRows;
        }

        private void fillRecord(int index, Object[] record) {
            for (int i = 0; i < names.size(); i++) {
                record[i] = values.get(i).get(index);
            }
        }

        private void fillRecord(int index, Map<String, Object> record) {
            int colIndex = 0;
            for (String name : names) {
                record.put(name, values.get(colIndex).get(index));
                colIndex++;
            }
        }
    }
}
