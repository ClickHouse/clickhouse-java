package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseDataType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NativeStreamReader extends AbstractRowBinaryReader {

    private Block currentBlock;

    private int blockRowIndex;

    public NativeStreamReader(InputStream inputStream, QuerySettings settings) {
        super(inputStream, settings);
    }

    @Override
    public void readToMap(Map<String, Object> record, TableSchema schema) throws IOException {
        if (currentBlock == null || blockRowIndex >= currentBlock.getnRows()) {
            readBlock();
        }

        currentBlock.fillRecord(blockRowIndex, record);
        blockRowIndex++;
    }

    @Override
    public void reset() throws IOException {

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
                Object value = readValue(dataType);
                values.add(value);
            }
            currentBlock.add(values);
        }
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
