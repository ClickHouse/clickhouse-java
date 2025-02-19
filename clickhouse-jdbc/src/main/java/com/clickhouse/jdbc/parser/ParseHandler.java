package com.clickhouse.jdbc.parser;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Deprecated
public abstract class ParseHandler {
    /**
     * Handle macro like "#include('/tmp/template.sql')".
     * 
     * @param name       name of the macro
     * @param parameters parameters
     * @return output of the macro, could be null or empty string
     */
    public String handleMacro(String name, List<String> parameters) {
        return null;
    }

    /**
     * Handle parameter.
     * 
     * @param cluster     cluster
     * @param database    database
     * @param table       table
     * @param columnIndex columnIndex(starts from 1 not 0)
     * @return parameter value
     */
    public String handleParameter(String cluster, String database, String table, int columnIndex) {
        return null;
    }

    /**
     * Hanlde statemenet.
     * 
     * @param sql               sql statement
     * @param stmtType          statement type
     * @param cluster           cluster
     * @param database          database
     * @param table             table
     * @param compressAlgorithm compression algorithm
     * @param compressLevel     compression level
     * @param format            format
     * @param input             input
     * @param file              infile or outfile
     * @param parameters        positions of parameters
     * @param positions         keyword positions
     * @param settings          settings
     * @param tempTables        temporary tables
     * @return sql statement, or null means no change
     */
    public ClickHouseSqlStatement handleStatement(String sql, StatementType stmtType, String cluster, String database,
            String table, String input, String compressAlgorithm, String compressLevel, String format, String file,
            List<Integer> parameters, Map<String, Integer> positions, Map<String, String> settings,
            Set<String> tempTables) {
        return null;
    }
}
