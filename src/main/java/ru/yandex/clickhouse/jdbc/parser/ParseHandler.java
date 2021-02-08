package ru.yandex.clickhouse.jdbc.parser;

import java.util.List;

public interface ParseHandler {
    /**
     * Handle macro like "#include('/tmp/template.sql')".
     * 
     * @param name       name of the macro
     * @param parameters parameters
     * @return output of the macro, could be null or empty string
     */
    String handleMacro(String name, List<String> parameters);

    /**
     * Handle parameter.
     * 
     * @param cluster     cluster
     * @param database    database
     * @param table       table
     * @param columnIndex columnIndex(starts from 1 not 0)
     * @return parameter value
     */
    String handleParameter(String cluster, String database, String table, int columnIndex);
}
