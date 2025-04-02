package com.clickhouse.benchmark.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class DataSetGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(DataSetGenerator.class);
    public static DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Formatter for the Date type.
     */
    public static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * Formatter for the DateTime type with nanoseconds.
     */
    public static DateTimeFormatter DATETIME_WITH_NANOS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn");
    public static void main(String[] args) throws IOException {

        String inputSQL = null;
        int rows = 10;
        String datasetName = "dataset_" + System.currentTimeMillis();
        // Simple command-line parsing
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-input":
                    if (i + 1 < args.length) {
                        inputSQL = args[++i];
                    }
                    break;
                case "-rows":
                    if (i + 1 < args.length) {
                        rows = Integer.parseInt(args[++i]);
                    }
                    break;
                case "-name":
                    if (i + 1 < args.length) {
                        datasetName = args[++i];
                    }
                    break;
                default:
                    // Ignore unknown flags
                    break;
            }
        }

        if (inputSQL == null) {
            System.out.println("Error: No original Create Statement was provided.");
            return;
        }

        try {
            Path path = Paths.get(inputSQL);
            System.out.println("SQL file path: " + path + " cwd: " + System.getProperty("user.dir"));
            inputSQL = new String(Files.readAllBytes(path));
        }catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Dataset name: " + datasetName);

        // Parse the SQL create table statement
        List<Column> originalColumns;
        try {
            originalColumns = parseCreateTable(inputSQL);
        } catch (Exception e) {
            System.out.println("Error parsing SQL: " + e.getMessage());
            return;
        }

        if (originalColumns.isEmpty()) {
            System.out.println("No valid column definitions found.");
            return;
        }

        // Generate abstract CREATE TABLE statement
        String abstractSQL = generateAbstractCreateTable(originalColumns);
        System.out.println("Table statement\n" + abstractSQL);

        // Setup series generation for date/time if provided
        boolean useSeries = false;
        LocalDateTime seriesStart = null;
        Duration seriesInterval = Duration.ofSeconds(1);

        File outputFile = new File(datasetName + ".csv");
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile.toPath())) {
            writer.write(datasetName + "\n");
            writer.write("--Create Table Statement\n");
            writer.write(abstractSQL + "\n");
            writer.write("--End Create Table Statement\n");
            writer.write("--Metadata\n");
            writer.write("columns=" + originalColumns.size() + "\n");
            writer.write("rows=" + rows + "\n");
            writer.write("--End Metadata\n");

            writer.write("DATA>>\n");
            Random rand = new Random();

            for (int i = 0; i < rows; i++) {
                List<String> row = generateRow(originalColumns, i, seriesStart, seriesInterval, useSeries, rand);
                writer.write(String.join(",", row) + "\n");
            }
        } catch (IOException e) {
            System.out.println("Error writing to file: " + e.getMessage());
        }
    }

    private static List<Column> parseCreateTable(String sql) throws Exception {

        List<Column> columns = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new StringReader(sql));

        Pattern linePattern = Pattern.compile("\\`?([\\w_]+)\\`?\\s+(.+)");
        int columnCount = 0;
        for (String line : reader.lines().collect(Collectors.toList())) {
            line = line.trim();
            if (line.startsWith("CREATE TABLE") || line.startsWith("CREATE TEMPORARY TABLE") || line.trim().startsWith("--")) {
                continue; // Skip the CREATE TABLE line
            }
            if (line.startsWith(")")) {
                break; // End of column definitions
            }
            if (line.endsWith(",")) {
                line = line.substring(0, line.length() - 1); // Remove trailing comma
            }

            // Regex to match column definitions
            Matcher lineMatcher = linePattern.matcher(line);
            if (lineMatcher.find()) {
                String colName = lineMatcher.group(1).trim();
                String colType = lineMatcher.group(2).trim();
                Column col = new Column("col" + (columnCount++), colType);
                columns.add(col);
                System.out.println(colName + " -> " + col.name + ", Type: " + colType);
            }
        }

        return columns;
    }

    // Generates an abstract CREATE TABLE statement with column names replaced by col1, col2, ...
    private static String generateAbstractCreateTable(List<Column> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("(\n");
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            sb.append("   col").append(i + 1).append(" ").append(col.type);
            if (i < columns.size() - 1) {
                sb.append(",\n");
            } else {
                sb.append("\n");
            }
        }
        sb.append(") Engine = MergeTree ORDER BY ();");
        return sb.toString();
    }


    private static List<String> generateRow(List<Column> columns, int rowIndex, LocalDateTime seriesStart,
                                            Duration seriesInterval, boolean useSeries, Random rand) {
        List<String> row = new ArrayList<>();
        for (Column col : columns) {
            String[] colDef = col.type.split("[\\s\\(\\)]+");

            ClickHouseDataTypesShort chType;
            try {
                chType = ClickHouseDataTypesShort.valueOf(colDef[0]);
            } catch (IllegalArgumentException e) {
                LOG.error("Unknown type: " + colDef[0]);
                chType = ClickHouseDataTypesShort.String; // Default to String if unknown
            }

            if (chType.hasParameters()) {
                // Handle types with parameters (e.g., Decimal, Array, etc.)
                String param = colDef[1];
                if (param.startsWith("(") && param.endsWith(")")) {
                    param = param.substring(1, param.length() - 1);
                }
                String[] params = param.split(",");
                if (params.length > 0) {
                    // Use the first parameter for generating values
                    col.type = chType.name() + "(" + params[0] + ")";
                }
            }

            switch (chType) {
                case Int8:
                    row.add(String.valueOf(rand.nextInt(256) - 128));
                    break;
                case UInt8:
                    row.add(String.valueOf(rand.nextInt(256)));
                    break;
                case Int16:
                    row.add(String.valueOf(rand.nextInt(65536) - 32768));
                    break;
                case UInt16:
                    row.add(String.valueOf(rand.nextInt(65536)));
                    break;
                case Int32:
                    row.add(String.valueOf(rand.nextInt()));
                    break;
                case UInt32:
                    row.add(String.valueOf(Math.abs(rand.nextLong())));
                    break;
                case Int64:
                    row.add(String.valueOf(rand.nextLong()));
                    break;
                case UInt64:
                    row.add(String.valueOf(Math.abs(rand.nextLong())));
                    break;
                case Float32:
                    row.add(String.valueOf(rand.nextFloat()));
                    break;
                case Float64:
                    row.add(String.valueOf(rand.nextDouble()));
                    break;
                case String:
                    row.add(randomString(10, rand));
                    break;
                case DateTime:
                case DateTime32:
                case DateTime64:
                case Date:
                    if (useSeries && seriesStart != null) {
                        LocalDateTime dateTime = seriesStart.plus(seriesInterval.multipliedBy(rowIndex));
                        row.add(dateTime.format(chType == ClickHouseDataTypesShort.Date ? DATE_FORMATTER : DATETIME_FORMATTER));
                    } else {
                        LocalDateTime dateTime = LocalDateTime.now().minusDays(rand.nextInt(365));
                        row.add(dateTime.format(chType == ClickHouseDataTypesShort.Date ? DATE_FORMATTER : DATETIME_FORMATTER));
                    }
                    break;
                case Decimal:
                    if (colDef.length > 1) {
                        int scale = 10;
                        double value = rand.nextDouble() * Math.pow(10, scale);
                        row.add(String.format("%." + scale + "f", value));
                    } else {
                        row.add("0.0");
                    }
                    break;
                default:
                    // Handle other types as needed
                    row.add("NULL");

            }
        }

        return row;
    }

    private static String randomString(int n, Random rand) {
        String letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            int index = rand.nextInt(letters.length());
            sb.append(letters.charAt(index));
        }
        return sb.toString();
    }


    // Simple Column class to hold column name and type
    static class Column {
        String name;
        String type;

        Column(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }
}
