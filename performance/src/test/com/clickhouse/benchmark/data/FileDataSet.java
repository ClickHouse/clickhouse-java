package com.clickhouse.benchmark.data;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.Table;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FileDataSet implements DataSet{

    private static final Logger LOGGER = LoggerFactory.getLogger(FileDataSet.class);
    private final String name;

    private final String createTableStmt;
    private final TableSchema schema;

    private final Map<String, String> metadata = new HashMap<>();

    private List<byte[]> lines =null;

    private List<Map<String, Object>> data;

    public FileDataSet(String filePath) {
        File srcFile = new File(filePath);

        try (BufferedReader r = new BufferedReader(new java.io.FileReader(srcFile))) {
            String line;

            String name = null;
            StringBuilder createStatement = null;
            boolean doneCreateStatement = false;
            boolean isMetadata = false;
            boolean isData = false;
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
                    int rows = Integer.parseInt(metadata.get("rows"));
                    lines = new ArrayList<>(rows);
                    isData = true;
                } else if (isData) {
                    line = line + "\n"; // not optimal but ok for now.
                    lines.add(line.getBytes());
                } else {
                    throw new IllegalArgumentException("Invalid file format: " + srcFile.getAbsolutePath());
                }

            }
            this.name = name;
            this.createTableStmt = createStatement != null ? createStatement.toString() : null;
            this.schema = DataSets.parseSchema(createTableStmt);
            LOGGER.info("Read " + lines.size() + " lines from " + srcFile.getAbsolutePath());

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to read file: " + srcFile.getAbsolutePath(), e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getTableName() {
        return "data_" + name;
    }

    @Override
    public int getSize() {
        return lines.size();
    }

    @Override
    public String getCreateTableString() {
        return createTableStmt;
    }
    @Override
    public String getTrucateTableString() {
        return "TRUNCATE TABLE " + getTableName();
    }

    private TableSchema tableSchema = new TableSchema();
    @Override
    public TableSchema getSchema() {
        return tableSchema;
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
        List<ClickHouseColumn> columns = tableSchema.getColumns();
        data = new ArrayList<>(records.size());
        for (ClickHouseRecord record : records) {
            Iterator<ClickHouseValue> vIter = record.iterator();
            int i = 0;
            Map<String, Object> row = new HashMap<>();
            while (vIter.hasNext()) {
                ClickHouseValue v = vIter.next();
                row.put(columns.get(i++).getColumnName(), v.asObject());
            }
            data.add(row);
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
        for (ClickHouseColumn column : dataProcessor.getColumns()) {
            tableSchema.addColumn(column.getColumnName(), column.getOriginalTypeName());
        }
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
