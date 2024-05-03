package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseDataType;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class NativeFormatReader extends AbstractBinaryFormatReader {

    private Block currentBlock;

    private int blockRowIndex;

    public NativeFormatReader(InputStream inputStream, QuerySettings settings) {
        super(inputStream, settings, null);
    }

    @Override
    public void readRecord(Map<String, Object> record) throws IOException {
        if (currentBlock == null || blockRowIndex >= currentBlock.getnRows()) {
            readBlock();
        }

        currentBlock.fillRecord(blockRowIndex, record);
        blockRowIndex++;
    }

    private void readBlock() throws IOException {
        int nColumns = chInputStream.readVarInt();
        int nRows = chInputStream.readVarInt();

        List<String> names = new ArrayList<>(nColumns);
        List<String> types = new ArrayList<>(nColumns);
        currentBlock = new Block(names, types, nRows);
        for (int i = 0; i < nColumns; i++) {
            names.add(chInputStream.readUnicodeString());
            types.add(chInputStream.readUnicodeString());

            ClickHouseDataType dataType = ClickHouseDataType.of(types.get(i));
            List<Object> values = new ArrayList<>(nRows);
            for (int j = 0; j < nRows; j++) {
                Object value = binaryStreamReader.readValue(dataType);
                values.add(value);
            }
            currentBlock.add(values);
        }
        blockRowIndex = 0;
    }

    @Override
    public <T> T readValue(int colIndex) {
        return (T) currentRecord.get(getSchema().indexToName(colIndex));
    }

    @Override
    public <T> T readValue(String colName) {
        return (T) currentRecord.get(colName);
    }

    @Override
    public boolean next() {
        if (hasNext) {
            try {
                readRecord(currentRecord);
            } catch (EOFException e) {
                hasNext = false;
                return false;
            } catch (IOException e) {
                throw new ClientException("Failed to read next block", e);
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

        private void fillRecord(int index, Map<String, Object> record) {
            int colIndex = 0;
            for (String name : names) {
                record.put(name, values.get(colIndex).get(index));
                colIndex++;
            }
        }
    }
}
