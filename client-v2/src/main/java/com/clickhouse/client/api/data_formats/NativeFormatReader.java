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
 * Reads a result in the {@code Native} format.
 *
 * <p><b>Stream layout.</b> A Native result is a sequence of blocks. There is no header and no trailer. The server
 * sends an empty result as an empty stream, but the reader also accepts blocks without rows. One block is:
 * <pre>
 * VarUInt   number of columns
 * VarUInt   number of rows
 * for each column:
 *   String  column name
 *   String  column type
 *   bytes   values of all rows of the column, in the Native (column-oriented) layout of the type
 * </pre>
 * All blocks of one result have the same columns: the same number, in the same order, with the same names and
 * types.
 *
 * <p><b>How rows are read.</b>
 * <ul>
 *     <li>{@code readBlock()} decodes one full block into memory, column by column. The constructor reads the first
 *     block, so the schema is known before the first row is read. For an empty stream, {@link #getSchema()} returns
 *     {@code null}. The schema comes from the first block and does not change.</li>
 *     <li>Rows are taken from the block in memory by their index in the block. The next block is read only when all
 *     rows of the current block were taken. Blocks without rows are skipped.</li>
 *     <li>{@link #hasNext()} reads a block only when the current block has no row left. Thus, more calls before
 *     {@link #next()} read nothing and give the same result. Reading the next block does not change the values of
 *     the current row. {@link #next()} takes the row and moves to the next index. It does not read ahead.
 *     {@link #readRecord(Map)} takes rows in the same way.</li>
 *     <li>The result ends when the stream ends before or inside the column count of the next block. If the stream
 *     ends after the column count, reading fails.</li>
 *     <li>After a read failure, {@link #hasNext()} returns {@code false}.</li>
 * </ul>
 * This reader does not use the one-row read-ahead of {@link AbstractBinaryFormatReader}.
 *
 * <p><b>Block check: "the reader is not at a block boundary".</b> The server does not send a block with columns
 * that are different from the first block. Thus, the reader compares the column count, the names, and the types of
 * each later block with the first block. If they are different, the reader did not read the previous block to its
 * end, and it reads other bytes as a block header. Usual causes:
 * <ul>
 *     <li>A column of the previous block uses a Native layout that this reader does not decode, and the reader read
 *     a wrong number of bytes for it. Examples: {@code LowCardinality}, and some nested {@code Array}, {@code Map},
 *     and {@code Tuple} types.</li>
 *     <li>The server wrote an error message into the stream.</li>
 * </ul>
 * Reading then fails with a {@link ClientException}, and no row of that block is returned. Rows of the block that was
 * decoded with a wrong layout can have wrong values. Some wrong layouts read the correct number of bytes, and this
 * check does not find them. To read such columns, use a {@code RowBinary} format, for example
 * {@code RowBinaryWithNamesAndTypes}.
 *
 * <p>For the backward compatibility server will not send TZ id in column type. Client should send version to a server
 * to get the correct column type.
 * (see: https://github.com/ClickHouse/ClickHouse/issues/38209)
 */
public class NativeFormatReader extends AbstractBinaryFormatReader {

    /** End of the message of a failed block check. It refers to the class Javadoc, which explains the check. */
    private static final String NOT_AT_BLOCK_BOUNDARY = ". The reader is not at a block boundary, see the Javadoc of "
            + NativeFormatReader.class.getName();

    /** Block that rows are taken from. {@code null} at the end of the result and after a read failure. */
    private Block currentBlock;

    /** Index of the next row to take from {@link #currentBlock}. */
    private int blockRowIndex;

    /** Column names of the first block, {@code null} before it. The rows of all blocks use these names. */
    private List<String> columnNames;

    /** Column types of the first block, as the server wrote them. All later blocks must have the same types. */
    private List<String> columnTypes;

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
        if (!hasRowInBlock()) {
            return false;
        }

        currentBlock.fillRecord(blockRowIndex, record);
        blockRowIndex++;
        return true;
    }

    @Override
    public boolean hasNext() {
        try {
            return hasRowInBlock();
        } catch (IOException e) {
            throw new ClientException("Failed to read block", e);
        }
    }

    @Override
    public Map<String, Object> next() {
        if (!hasNext()) {
            return null;
        }
        currentBlock.fillRecord(blockRowIndex, currentRecord);
        blockRowIndex++;
        return wrapRecord(currentRecord);
    }

    @Override
    protected boolean readRecord(Object[] record) throws IOException {
        if (!hasRowInBlock()) {
            return false;
        }

        currentBlock.fillRecord(blockRowIndex, record);
        blockRowIndex++;
        return true;
    }

    /**
     * Makes sure that {@link #currentBlock} has a row at {@link #blockRowIndex} that was not taken yet. Reads the next
     * block only when all rows of the current block were taken, and skips blocks without rows. If the current block
     * has a row, this method reads nothing. Thus, it gives the same result for each call until the row is taken.
     *
     * @return {@code true} if there is a row to take, {@code false} at the end of the result
     * @throws IOException if the next block cannot be read. The reader is then at the end of the result.
     * @throws ClientException if the next block has other columns than the first block, or a column that this reader
     *         does not decode. The reader is then at the end of the result.
     */
    private boolean hasRowInBlock() throws IOException {
        try {
            while (currentBlock != null && blockRowIndex >= currentBlock.getnRows()) {
                readBlock();
            }
        } catch (IOException | RuntimeException e) {
            currentBlock = null;
            endReached();
            throw e;
        }
        return currentBlock != null;
    }

    /**
     * Reads the next block into {@link #currentBlock} and sets {@link #blockRowIndex} to its first row. If the stream
     * ends before the block, sets {@link #currentBlock} to {@code null}. The first block sets the schema. A later block
     * must have the same columns as the first block.
     *
     * @throws IOException if the stream cannot be read or ends after the column count of the block
     * @throws ClientException if the block has other columns than the first block (the reader is then not at a block
     *         boundary), or a column that this reader does not decode
     */
    private void readBlock() throws IOException {
        int nColumns;
        try {
            nColumns = BinaryStreamReader.readVarInt(input);
        } catch (EOFException e) {
            currentBlock = null;
            endReached();
            return;
        }
        int nRows = BinaryStreamReader.readVarInt(input);

        boolean firstBlock = columnNames == null;
        if (!firstBlock && nColumns != columnNames.size()) {
            throw new ClientException("Failed to read block: it has " + nColumns + " columns, the first block has "
                    + columnNames.size() + NOT_AT_BLOCK_BOUNDARY);
        }
        List<ClickHouseColumn> columns = firstBlock ? new ArrayList<>(nColumns) : getSchema().getColumns();
        if (firstBlock) {
            columnNames = new ArrayList<>(nColumns);
            columnTypes = new ArrayList<>(nColumns);
        }
        Block block = new Block(columnNames, nRows);
        for (int i = 0; i < nColumns; i++) {
            String name = BinaryStreamReader.readString(input);
            String type = BinaryStreamReader.readString(input);
            ClickHouseColumn column;
            if (firstBlock) {
                column = ClickHouseColumn.of(name, type);
                columns.add(column);
                columnNames.add(column.getColumnName());
                columnTypes.add(type);
            } else if (name.equals(columnNames.get(i)) && type.equals(columnTypes.get(i))) {
                column = columns.get(i);
            } else {
                // Do not put the name and the type into the message: they were read from wrong bytes.
                throw new ClientException("Failed to read block: column " + (i + 1)
                        + " has a different name or type than in the first block" + NOT_AT_BLOCK_BOUNDARY);
            }

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
            } else if (isGeo(column)) {
                // Native writes a geo column column-major: a Point as the two Float64 sub-columns of its
                // tuple, and every other geo type as cumulative offsets followed by the flattened elements
                // of the level below. The per-row decoders reached through readValue() read the RowBinary
                // layout instead, which scrambles a Point block and desynchronizes the columns that follow.
                values = binaryStreamReader.readGeoNative(column, nRows);
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
                    values.add(binaryStreamReader.readArray(column, len));
                    prevOffset = offsets[j];
                }
            } else if (column.isNullable() && !column.isLowCardinality()) {
                // Native encodes a Nullable column as a null map of nRows bytes followed by the values of
                // all rows; a row marked as null still has a placeholder value in the values section.
                // RowBinary, in contrast, prefixes every value with its own marker - that marker is what
                // readValue() consumes, so it must not be read here.
                values = new ArrayList<>(nRows);
                boolean[] nulls = new boolean[nRows];
                for (int j = 0; j < nRows; j++) {
                    nulls[j] = binaryStreamReader.readByte() == 1;
                }
                // Nothing (the type of a bare NULL) has no value bytes in RowBinary, but Native still writes
                // one placeholder byte per row, so it has to be consumed here.
                boolean nothing = column.getDataType() == ClickHouseDataType.Nothing;
                for (int j = 0; j < nRows; j++) {
                    Object value;
                    if (nothing) {
                        binaryStreamReader.readByte();
                        value = null;
                    } else {
                        value = binaryStreamReader.readValueWithoutNullMarker(column);
                    }
                    values.add(nulls[j] ? null : value);
                }
            } else {
                values = new ArrayList<>(nRows);
                for (int j = 0; j < nRows; j++) {
                    Object value = binaryStreamReader.readValue(column);
                    values.add(value);
                }
            }
            block.add(values);
        }
        if (firstBlock) {
            // Later blocks have the same columns, so the schema and the row buffers that it creates stay the same.
            setSchema(new TableSchema(columns));
        }

        currentBlock = block;
        blockRowIndex = 0;
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
     * Returns {@code true} for a geo column ({@code Point}, {@code Ring}, {@code LineString},
     * {@code MultiPoint}, {@code Polygon}, {@code MultiLineString} or {@code MultiPolygon}). Geo types
     * are their own data types rather than {@code Array}/{@code Tuple}, so they do not reach the
     * columnar branches of {@link #readBlock} on their own and need their own Native decoder
     * ({@link BinaryStreamReader#readGeoNative(ClickHouseColumn, int)}).
     */
    private static boolean isGeo(ClickHouseColumn column) {
        switch (column.getDataType()) {
            case Point:
            case Ring:
            case LineString:
            case MultiPoint:
            case Polygon:
            case MultiLineString:
            case MultiPolygon:
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

    /** Decoded values of one block: one list of {@code nRows} values for each column. */
    private static class Block {
        final List<String> names;

        final List<List<Object>> values = new ArrayList<>();
        final int nRows;

        Block(List<String> names, int nRows) {
            this.names = names;
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
