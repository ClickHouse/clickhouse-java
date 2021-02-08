package ru.yandex.clickhouse.jdbc.parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ClickHouseSqlStatement {
    public static final String DEFAULT_DATABASE = "system";
    public static final String DEFAULT_TABLE = "unknown";
    public static final List<Integer> DEFAULT_PARAMETERS = Collections.emptyList();
    public static final Map<String, Integer> DEFAULT_POSITIONS = Collections.emptyMap();
    public static final Map<Integer, String> DEFAULT_VARIABLES = Collections.emptyMap();

    public static final String KEYWORD_FORMAT = "FORMAT";
    public static final String KEYWORD_TOTALS = "TOTALS";
    public static final String KEYWORD_VALUES = "VALUES";

    private final String sql;
    private final StatementType stmtType;
    private final String cluster;
    private final String database;
    private final String table;
    private final String format;
    private final String outfile;
    private final Map<String, Integer> positions;

    public ClickHouseSqlStatement(String sql) {
        this(sql, StatementType.UNKNOWN, null, null, null, null, null, null);
    }

    public ClickHouseSqlStatement(String sql, StatementType stmtType) {
        this(sql, stmtType, null, null, null, null, null, null);
    }

    public ClickHouseSqlStatement(String sql, StatementType stmtType, String cluster, String database, String table,
            String format, String outfile, Map<String, Integer> positions) {
        this.sql = sql;
        this.stmtType = stmtType;

        this.cluster = cluster;
        this.database = database;
        this.table = table == null || table.isEmpty() ? DEFAULT_TABLE : table;
        this.format = format;
        this.outfile = outfile;

        if (positions != null && positions.size() > 0) {
            Map<String, Integer> p = new HashMap<>();
            for (Entry<String, Integer> e : positions.entrySet()) {
                String keyword = e.getKey();
                Integer position = e.getValue();

                if (keyword != null && position != null) {
                    p.put(keyword.toUpperCase(), position);
                }
            }
            this.positions = Collections.unmodifiableMap(p);
        } else {
            this.positions = DEFAULT_POSITIONS;
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
        return this.stmtType.getOperationType() == OperationType.READ && !this.hasOutfile();
    }

    public boolean isMutation() {
        return this.stmtType.getOperationType() == OperationType.WRITE || this.hasOutfile();
    }

    public boolean isIdemponent() {
        return this.isQuery();
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

    public String getFormat() {
        return this.format;
    }

    public String getOutfile() {
        return this.outfile;
    }

    public boolean hasFormat() {
        return this.format != null && !this.format.isEmpty();
    }

    public boolean hasOutfile() {
        return this.outfile != null && !this.outfile.isEmpty();
    }

    public boolean hasWithTotals() {
        return this.positions.containsKey(KEYWORD_TOTALS);
    }

    public boolean hasValues() {
        return this.positions.containsKey(KEYWORD_VALUES);
    }

    public int getStartPosition(String keyword) {
        int position = -1;

        if (!this.positions.isEmpty() && keyword != null) {
            Integer p = this.positions.get(keyword.toUpperCase());
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append('[').append(stmtType.name()).append(']').append(" cluster=").append(cluster).append(", database=")
                .append(database).append(", table=").append(table).append(", format=").append(format)
                .append(", outfile=").append(outfile).append(", positions=").append(positions).append("\nSQL:\n")
                .append(sql);

        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
        result = prime * result + ((database == null) ? 0 : database.hashCode());
        result = prime * result + ((format == null) ? 0 : format.hashCode());
        result = prime * result + ((outfile == null) ? 0 : outfile.hashCode());
        result = prime * result + ((positions == null) ? 0 : positions.hashCode());
        result = prime * result + ((sql == null) ? 0 : sql.hashCode());
        result = prime * result + ((stmtType == null) ? 0 : stmtType.hashCode());
        result = prime * result + ((table == null) ? 0 : table.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClickHouseSqlStatement other = (ClickHouseSqlStatement) obj;
        if (cluster == null) {
            if (other.cluster != null)
                return false;
        } else if (!cluster.equals(other.cluster))
            return false;
        if (database == null) {
            if (other.database != null)
                return false;
        } else if (!database.equals(other.database))
            return false;
        if (format == null) {
            if (other.format != null)
                return false;
        } else if (!format.equals(other.format))
            return false;
        if (outfile == null) {
            if (other.outfile != null)
                return false;
        } else if (!outfile.equals(other.outfile))
            return false;
        if (positions == null) {
            if (other.positions != null)
                return false;
        } else if (!positions.equals(other.positions))
            return false;
        if (sql == null) {
            if (other.sql != null)
                return false;
        } else if (!sql.equals(other.sql))
            return false;
        if (stmtType != other.stmtType)
            return false;
        if (table == null) {
            if (other.table != null)
                return false;
        } else if (!table.equals(other.table))
            return false;
        return true;
    }
}
