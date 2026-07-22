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

            if (containsQBit(column)) {
                // QBit is transmitted in the Native format using its internal bit-transposed
                // Tuple(FixedString(...)) layout, which is NOT the Array(element_type)-like
                // representation used in RowBinary (the only representation this reader decodes for
                // QBit). Reading it through the columnar/per-row paths below would misread those bytes
                // and desynchronize the block, corrupting the columns that follow. Fail loudly instead
                // of silently decoding garbage. This also covers a QBit nested inside another type
                // (e.g. Map(String, QBit(...))). QBit can be read through a RowBinary format.
                throw new ClientException("Reading column '" + column.getColumnName() + "' ("
                        + column.getOriginalTypeName() + ") from the Native format is not supported "
                        + "because it contains a QBit type: QBit is serialized in the Native format "
                        + "using an internal layout this reader does not decode. Use a RowBinary format "
                        + "(e.g. RowBinaryWithNamesAndTypes) to read QBit values");
            }

            List<Object> values = new ArrayList<>(nRows);
            if (column.isArray()) {
                int[] sizes = new int[nRows];
                for (int j = 0; j < nRows; j++) {
                    sizes[j] = Math.toIntExact(binaryStreamReader.readLongLE());
                }
                for (int j = 0; j < nRows; j++) {
                    values.add(binaryStreamReader.readArrayItem(column.getNestedColumns().get(0), sizes[0]));
                }
            } else {
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
     * Returns {@code true} if {@code column} is a {@code QBit} or contains a {@code QBit} anywhere in
     * its nested type tree (e.g. {@code Array(QBit(...))}, {@code Tuple(..., QBit(...))},
     * {@code Map(String, QBit(...))}). {@code QBit} uses a different, internal wire layout in the
     * Native format than in RowBinary, so this reader cannot decode it and rejects such columns
     * up-front rather than misreading the block. {@code Nullable}/{@code LowCardinality} wrappers are
     * flags on the column, so a wrapped {@code QBit} still reports {@code dataType == QBit} here.
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
