package com.clickhouse.jdbc.parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;

@Deprecated
public class ClickHouseSqlStatement {
    public static final String DEFAULT_DATABASE = "system";
    public static final String DEFAULT_TABLE = "unknown";

    public static final String KEYWORD_DATABASE = "DATABASE";
    public static final String KEYWORD_EXISTS = "EXISTS";
    public static final String KEYWORD_FORMAT = "FORMAT";
    public static final String KEYWORD_REPLACE = "REPLACE";
    public static final String KEYWORD_TOTALS = "TOTALS";
    public static final String KEYWORD_VALUES = "VALUES";

    public static final String KEYWORD_TABLE_COLUMNS_START = "ColumnsStart";
    public static final String KEYWORD_TABLE_COLUMNS_END = "ColumnsEnd";
    public static final String KEYWORD_VALUES_START = "ValuesStart";
    public static final String KEYWORD_VALUES_END = "ValuesEnd";

    private final String sql;
    private final StatementType stmtType;
    private final String cluster;
    private final String database;
    private final String table;
    private final String input;
    private final String compressAlgorithm;
    private final String compressLevel;
    private final String format;
    private final String file;
    private final List<Integer> parameters;
    private final Map<String, Integer> positions;
    private final Map<String, String> settings;
    private final Set<String> tempTables;

    public ClickHouseSqlStatement(String sql) {
        this(sql, StatementType.UNKNOWN, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    public ClickHouseSqlStatement(String sql, StatementType stmtType) {
        this(sql, stmtType, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    public ClickHouseSqlStatement(String sql, StatementType stmtType, String cluster, String database, String table,
            String input, String compressAlgorithm, String compressLevel, String format, String file,
            List<Integer> parameters, Map<String, Integer> positions, Map<String, String> settings,
            Set<String> tempTables) {
        this.sql = sql;
        this.stmtType = stmtType;

        this.cluster = cluster;
        this.database = database;
        this.table = table == null || table.isEmpty() ? DEFAULT_TABLE : table;
        this.input = input;
        this.compressAlgorithm = compressAlgorithm;
        this.compressLevel = compressLevel;
        this.format = format;
        this.file = file;

        if (parameters != null && !parameters.isEmpty()) {
            this.parameters = Collections.unmodifiableList(parameters);
        } else {
            this.parameters = Collections.emptyList();
        }

        if (positions != null && !positions.isEmpty()) {
            Map<String, Integer> p = new HashMap<>();
            for (Entry<String, Integer> e : positions.entrySet()) {
                String keyword = e.getKey();
                Integer position = e.getValue();

                if (keyword != null && position != null) {
                    p.put(keyword, position);
                }
            }
            this.positions = Collections.unmodifiableMap(p);
        } else {
            this.positions = Collections.emptyMap();
        }

        if (settings != null && !settings.isEmpty()) {
            Map<String, String> s = new LinkedHashMap<>();
            for (Entry<String, String> e : settings.entrySet()) {
                String key = e.getKey();
                String value = e.getValue();

                if (key != null && value != null) {
                    s.put(key, String.valueOf(e.getValue()));
                }
            }
            this.settings = Collections.unmodifiableMap(s);
        } else {
            this.settings = Collections.emptyMap();
        }

        if (tempTables != null && !tempTables.isEmpty()) {
            Set<String> s = new LinkedHashSet<>();
            s.addAll(tempTables);
            this.tempTables = Collections.unmodifiableSet(s);
        } else {
            this.tempTables = Collections.emptySet();
        }
    }

    public String getSQL() {
        return this.sql;
    }

    public boolean isRecognized() {
        return stmtType != StatementType.UNKNOWN;
    }

    public boolean isDDL() {
        return this.stmtType.getLanguageType() == LanguageType.DDL;
    }

    public boolean isDML() {
        return this.stmtType.getLanguageType() == LanguageType.DML;
    }

    public boolean isQuery() {
        return this.stmtType.getOperationType() == OperationType.READ && !this.hasFile();
    }

    public boolean isMutation() {
        return this.stmtType.getOperationType() == OperationType.WRITE || this.hasFile();
    }

    public boolean isTCL() {
        return this.stmtType.getLanguageType() == LanguageType.TCL;
    }

    public boolean isIdemponent() {
        boolean result = this.stmtType.isIdempotent() && !this.hasFile();

        if (!result) { // try harder
            switch (this.stmtType) {
                case ATTACH:
                case CREATE:
                case DETACH:
                case DROP:
                    result = positions.containsKey(KEYWORD_EXISTS) || positions.containsKey(KEYWORD_REPLACE);
                    break;

                default:
                    break;
            }
        }

        return result;
    }

    public LanguageType getLanguageType() {
        return this.stmtType.getLanguageType();
    }

    public OperationType getOperationType() {
        return this.stmtType.getOperationType();
    }

    public StatementType getStatementType() {
        return this.stmtType;
    }

    public String getCluster() {
        return this.cluster;
    }

    public String getDatabase() {
        return this.database;
    }

    public String getDatabaseOrDefault(String database) {
        return this.database == null ? (database == null ? DEFAULT_DATABASE : database) : this.database;
    }

    public String getTable() {
        return this.table;
    }

    public String getInput() {
        return this.input;
    }

    public String getCompressAlgorithm() {
        return this.compressAlgorithm;
    }

    public String getCompressLevel() {
        return this.compressLevel;
    }

    public String getFormat() {
        return this.format;
    }

    public String getFile() {
        return this.file;
    }

    public String getContentBetweenKeywords(String startKeyword, String endKeyword) {
        return getContentBetweenKeywords(startKeyword, endKeyword, 0);
    }

    public String getContentBetweenKeywords(String startKeyword, String endKeyword, int startOffset) {
        if (startOffset < 0) {
            startOffset = 0;
        }
        Integer startPos = positions.get(startKeyword);
        Integer endPos = positions.get(endKeyword);

        String content = "";
        if (startPos != null && endPos != null && startPos + startOffset < endPos) {
            content = sql.substring(startPos + startOffset, endPos);
        }

        return content;
    }

    public boolean containsKeyword(String keyword) {
        if (keyword == null || keyword.isEmpty()) {
            return false;
        }

        return positions.containsKey(keyword.toUpperCase(Locale.ROOT));
    }

    public boolean hasCompressAlgorithm() {
        return this.compressAlgorithm != null && !this.compressAlgorithm.isEmpty();
    }

    public boolean hasCompressLevel() {
        return this.compressLevel != null && !this.compressLevel.isEmpty();
    }

    public boolean hasFormat() {
        return this.format != null && !this.format.isEmpty();
    }

    public boolean hasInput() {
        return this.input != null && !this.input.isEmpty();
    }

    public boolean hasFile() {
        return this.file != null && !this.file.isEmpty();
    }

    public boolean hasSettings() {
        return !this.settings.isEmpty();
    }

    public boolean hasWithTotals() {
        return this.positions.containsKey(KEYWORD_TOTALS);
    }

    public boolean hasValues() {
        return this.positions.containsKey(KEYWORD_VALUES);
    }

    public boolean hasTempTable() {
        return !this.tempTables.isEmpty();
    }

    public List<Integer> getParameters() {
        return this.parameters;
    }

    public int getStartPosition(String keyword) {
        int position = -1;

        if (!this.positions.isEmpty() && keyword != null) {
            Integer p = this.positions.get(keyword.toUpperCase(Locale.ROOT));
            if (p != null) {
                position = p.intValue();
            }
        }

        return position;
    }

    public int getEndPosition(String keyword) {
        int position = getStartPosition(keyword);

        return position != -1 && keyword != null ? position + keyword.length() : position;
    }

    public Map<String, Integer> getPositions() {
        return this.positions;
    }

    public Map<String, String> getSettings() {
        return this.settings;
    }

    public Set<String> getTempTables() {
        return this.tempTables;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append('[').append(stmtType.name()).append(']').append(" cluster=").append(cluster).append(", database=")
                .append(database).append(", table=").append(table).append(", input=").append(input)
                .append(", compressAlgorithm=").append(compressAlgorithm).append(", compressLevel=")
                .append(compressLevel).append(", format=").append(format).append(", outfile=").append(file)
                .append(", parameters=").append(parameters).append(", positions=").append(positions)
                .append(", settings=").append(settings).append(", tempTables=").append(settings).append("\nSQL:\n")
                .append(sql);

        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((sql == null) ? 0 : sql.hashCode());
        result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
        result = prime * result + ((database == null) ? 0 : database.hashCode());
        result = prime * result + table.hashCode();
        result = prime * result + ((input == null) ? 0 : input.hashCode());
        result = prime * result + ((compressAlgorithm == null) ? 0 : compressAlgorithm.hashCode());
        result = prime * result + ((compressLevel == null) ? 0 : compressLevel.hashCode());
        result = prime * result + ((format == null) ? 0 : format.hashCode());
        result = prime * result + ((file == null) ? 0 : file.hashCode());
        result = prime * result + ((stmtType == null) ? 0 : stmtType.hashCode());

        result = prime * result + parameters.hashCode();
        result = prime * result + positions.hashCode();
        result = prime * result + settings.hashCode();
        result = prime * result + tempTables.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseSqlStatement other = (ClickHouseSqlStatement) obj;
        return stmtType == other.stmtType && Objects.equals(sql, other.sql) && Objects.equals(cluster, other.cluster)
                && Objects.equals(database, other.database) && Objects.equals(table, other.table)
                && Objects.equals(input, other.input) && Objects.equals(compressAlgorithm, other.compressAlgorithm)
                && Objects.equals(compressLevel, other.compressLevel) && Objects.equals(format, other.format)
                && Objects.equals(file, other.file) && parameters.equals(other.parameters)
                && positions.equals(other.positions) && settings.equals(other.settings)
                && tempTables.equals(other.tempTables);
    }
}
