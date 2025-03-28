package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;

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
                              BinaryStreamReader.ByteBufferAllocator byteBufferAllocator) {
        super(inputStream, settings, null, byteBufferAllocator);
        try {
            readBlock();
        } catch (IOException e) {
            throw new ClientException("Failed to read block", e);
        }
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
