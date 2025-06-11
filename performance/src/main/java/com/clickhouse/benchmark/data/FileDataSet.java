package com.clickhouse.benchmark.data;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.util.*;

public class FileDataSet implements DataSet{

    private static final Logger LOGGER = LoggerFactory.getLogger(FileDataSet.class);
    private final String name;

    private final String createTableStmt;
    private TableSchema schema = null;

    private final Map<String, String> metadata = new HashMap<>();

    private List<byte[]> lines =null;

    private List<Map<String, Object>> data;
    private List<List<Object>> dataOrdered;

    public FileDataSet(String filePath, int limit) {
        LOGGER.info("Reading file {}", filePath);
        File srcFile = new File(filePath);

        try (BufferedReader r = new BufferedReader(new java.io.FileReader(srcFile))) {
            String line;

            String name = null;
            StringBuilder createStatement = null;
            boolean doneCreateStatement = false;
            boolean isMetadata = false;
            boolean isData = false;
            int lineNumber = 0;
            while ((line = r.readLine()) != null) {
                if (name == null) {
                    name = line.trim();
                } else if (line.startsWith("--Create Table Statement")) {
                    createStatement = new StringBuilder();
                } else if (line.startsWith("--End Create Table Statement")) {
                    doneCreateStatement = true;
                } else if (!doneCreateStatement && createStatement != null) {
                    createStatement.append(line).append("\n");
                } else if (line.startsWith("--Metadata")) {
                    isMetadata = true;
                } else if (line.startsWith("--End Metadata")) {
                    isMetadata = false;
                } else if (isMetadata) {
                    String[] parts = line.split("=");
                    metadata.put(parts[0].toLowerCase().trim(), parts[1].trim());
                } else if (line.startsWith("DATA>>")) {
                    //Integer.parseInt(metadata.get("rows"))
                    lines = new ArrayList<>(limit);
                    isData = true;
                } else if (isData) {
                    if (lineNumber++ < limit) {//If limit > rows in file, we'll just read all the rows.
                        line = line + "\n"; // not optimal but ok for now.
                        lines.add(line.getBytes());
                    }
                } else {
                    throw new IllegalArgumentException("Invalid file format: " + srcFile.getAbsolutePath());
                }
            }
            this.name = name;
            this.createTableStmt = createStatement != null ? createStatement.toString() : null;
//            this.schema = DataSets.parseSchema(createTableStmt);
            LOGGER.info("Read {} lines from {}", lines.size(), srcFile.getAbsolutePath());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to read file: " + srcFile.getAbsolutePath(), e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getSize() {
        return lines.size();
    }

    @Override
    public String getCreateTableString(String tableName) {
        return String.format("CREATE TABLE IF NOT EXISTS %s %s", tableName, createTableStmt);
    }

    @Override
    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public List<ClickHouseFormat> supportedFormats() {
        return Collections.singletonList(ClickHouseFormat.CSV);
    }

    @Override
    public List<byte[]> getBytesList(ClickHouseFormat format) {
        return lines;
    }

    @Override
    public List<Map<String, Object>> getRows() {
        return data;
    }
    @Override
    public List<Map<String, Object>> getRowsLimit(int numRows) {
        return data.subList(0, numRows);
    }
    @Override
    public List<ClickHouseRecord> getClickHouseRecordsLimit(int numRows) {
        return clickHouseRecords.subList(0, numRows);
    }
    @Override
    public List<List<Object>> getRowsOrdered() {
        return dataOrdered;
    }
    @Override
    public ClickHouseFormat getFormat() {
        return ClickHouseFormat.CSV;
    }

    private List<ClickHouseRecord> clickHouseRecords;

    @Override
    public List<ClickHouseRecord> getClickHouseRecords() {
        return clickHouseRecords;
    }

    @Override
    public void setClickHouseRecords(List<ClickHouseRecord> records) {
        this.clickHouseRecords = records;
        List<ClickHouseColumn> columns = schema.getColumns();
        dataOrdered = new ArrayList<>(records.size());
        data = new ArrayList<>(records.size());
        for (ClickHouseRecord record : records) {
            Iterator<ClickHouseValue> vIter = record.iterator();
            int i = 0;
            Map<String, Object> row = new HashMap<>();
            List<Object> rowOrdered = new ArrayList<>(columns.size());
            while (vIter.hasNext()) {
                ClickHouseValue v = vIter.next();
                row.put(columns.get(i++).getColumnName(), v.asObject());

                rowOrdered.add(v.asObject());
            }
            data.add(row);
            dataOrdered.add(rowOrdered);

        }

    }

    private ClickHouseDataProcessor dataProcessor;

    @Override
    public ClickHouseDataProcessor getClickHouseDataProcessor() {
        return dataProcessor;
    }

    @Override
    public void setClickHouseDataProcessor(ClickHouseDataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
        List<ClickHouseColumn> columns = new ArrayList<>();
        for (ClickHouseColumn column : dataProcessor.getColumns()) {
            columns.add(ClickHouseColumn.of(column.getColumnName(), column.getOriginalTypeName()));
        }
        this.schema = new TableSchema(columns);
    }

    @Override
    public String toString() {
        return "FileDataSet{" +
                "name='" + name + '\'' +
                ", createTableStmt='" + createTableStmt + '\'' +
                ", metadata=" + metadata +
                '}';
    }
}
