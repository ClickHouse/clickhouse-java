package com.clickhouse.jdbc.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class StatementParser {


    public enum StatementType {
        SELECT, INSERT, DELETE, UPDATE, CREATE, DROP, ALTER, TRUNCATE, USE, SHOW, DESCRIBE, EXPLAIN, SET, KILL, OTHER, INSERT_INTO_SELECT
    }

    public static ParsedStatement parsedStatement(String sql) {
        return parseStatementType(sql);
    }

    public static ParsedStatement parsePreparedStatement(String sql) {
        ParsedStatement parsedStatement = parseStatementType(sql);
        String[] sqlSegments = splitStatement(sql);
        parsedStatement.setSqlSegments(sqlSegments);
        return parsedStatement;
    }

    private static final Pattern BLOCK_COMMENT = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);

    static ParsedStatement parseStatementType(String sql) {
        String trimmedSql = sql == null ? "" : sql.trim();
        if (trimmedSql.isEmpty()) {
            return new ParsedStatement(StatementType.OTHER, "");
        }

        trimmedSql = BLOCK_COMMENT.matcher(trimmedSql).replaceAll("").trim(); // remove comments
        String[] lines = trimmedSql.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String trimmedLine = lines[i].trim();
            //https://clickhouse.com/docs/en/sql-reference/syntax#comments
            if (!trimmedLine.startsWith("--") && !trimmedLine.startsWith("#!") && !trimmedLine.startsWith("#")) {
                String[] tokens = trimmedLine.split("\\s+");
                if (tokens.length == 0) {
                    continue;
                }
                switch (tokens[0].toUpperCase()) {
                    case "SELECT":
                        return new ParsedStatement(StatementType.SELECT, trimmedSql);
                    case "WITH":
                        return new ParsedStatement(StatementType.SELECT, trimmedSql);
                    case "INSERT":
                        // TODO: it is not optimal to re-parse current line
                        boolean hasSelect = false;
                        boolean prevWasInto = false;
                        boolean prevWasTable = false;
                        boolean hasValues = false;
                        boolean hasColumnList = false;
                        String tableName = "";
                        for (int j = i; j < lines.length; j++) {
                            trimmedLine = lines[j].trim();
                            if (!trimmedLine.startsWith("--") && !trimmedLine.startsWith("#!") && !trimmedLine.startsWith("#")) {
                                tokens = trimmedLine.split("\\s+");
                                if (tokens.length == 0) {
                                    continue;
                                }
                            }
                            for (String token : tokens) {
                                if (!hasColumnList && !hasValues && token.contains("(")) {
                                    hasColumnList = true;
                                }
                                if (token.equalsIgnoreCase("SELECT")) {
                                    hasSelect = true;
                                    break; // should be after we have found everything useful
                                } else if (token.equalsIgnoreCase("INTO")) {
                                    prevWasInto = true;
                                } else if (token.equalsIgnoreCase("TABLE")) {
                                    prevWasTable = true;
                                } else if (tableName.isEmpty() && (prevWasTable || prevWasInto)) {
                                    tableName = extractTableNameFromSegment(token);
                                } else if (token.equalsIgnoreCase("VALUES")) {
                                    hasValues = true;
                                }
                            }
                        }
                        ParsedStatement parsedStatement =
                                new ParsedStatement(hasSelect ? StatementType.INSERT_INTO_SELECT : StatementType.INSERT, trimmedSql);
                        parsedStatement.setTableName(tableName);
                        parsedStatement.setHasColumnList(hasColumnList);
                        return parsedStatement;
                    case "DELETE":
                        return new ParsedStatement(StatementType.DELETE, trimmedSql);
                    case "UPDATE":
                        return new ParsedStatement(StatementType.UPDATE, trimmedSql);
                    case "CREATE":
                        return new ParsedStatement(StatementType.CREATE, trimmedSql);
                    case "DROP":
                        return new ParsedStatement(StatementType.DROP, trimmedSql);
                    case "ALTER":
                        return new ParsedStatement(StatementType.ALTER, trimmedSql);
                    case "TRUNCATE":
                        return new ParsedStatement(StatementType.TRUNCATE, trimmedSql);
                    case "USE":
                        return new ParsedStatement(StatementType.USE, trimmedSql);
                    case "SHOW":
                        return new ParsedStatement(StatementType.SHOW, trimmedSql);
                    case "DESCRIBE":
                        return new ParsedStatement(StatementType.DESCRIBE, trimmedSql);
                    case "EXPLAIN":
                        return new ParsedStatement(StatementType.EXPLAIN, trimmedSql);
                    case "SET":
                        return new ParsedStatement(StatementType.SET, trimmedSql);
                    case "KILL":
                        return new ParsedStatement(StatementType.KILL, trimmedSql);
                    default:
                        return new ParsedStatement(StatementType.OTHER, trimmedSql);
                }
            }
        }

        return new ParsedStatement(StatementType.OTHER, trimmedSql);
    }


    // DOT NOT USE: part of a parsing algorithm.
    static String extractTableNameFromSegment(String segment) {
        StringBuilder tableNameBuilder = new StringBuilder();
        char openedQuote = 0;
        boolean escaping = false;

        for (char ch : segment.toCharArray()) {
            if (escaping) {
                tableNameBuilder.append(ch);
                escaping = false;
                continue;
            }
            if (openedQuote == ch) {
                break;
            }
            switch (ch) {
                case ' ':
                    continue;
                case '\\':
                    escaping = true;
                    tableNameBuilder.append(ch);
                    break;
                case '"':
                case '`':
                    openedQuote = ch;
                    continue;
                default:
                    tableNameBuilder.append(ch);
            }
        }
        return tableNameBuilder.toString();
    }

    public static class ParsedStatement {
        private final StatementType type;
        private final String trimmedSql;
        private String tableName;
        private String[] sqlSegments;
        private int argumentCount;
        private boolean hasColumnList;

        ParsedStatement(StatementType type, String trimmedSql) {
            this.type = type;
            this.trimmedSql = trimmedSql;
        }

        void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public void setSqlSegments(String[] sqlSegments) {
            this.sqlSegments = sqlSegments;
            this.argumentCount = sqlSegments == null ? 0 : sqlSegments.length -1;
        }

        public String[] getSqlSegments() {
            return this.sqlSegments;
        }

        public StatementType getType() {
            return this.type;
        }

        public String getTableName() {
            return tableName;
        }

        public int getArgumentCount() {
            return argumentCount;
        }

        public void setHasColumnList(boolean hasColumnList) {
            this.hasColumnList = hasColumnList;
        }

        public boolean hasColumnList() {
            return hasColumnList;
        }
    }

    private static String[] splitStatement(String sql) {
        List<String> segments = new ArrayList<>();
        char[] chars = sql.toCharArray();
        int segmentStart = 0;
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            if (c == '\'' || c == '"' || c == '`') {
                // string literal or identifier
                i = skip(chars, i + 1, c, true);
            } else if (c == '/' && lookahead(chars, i) == '*') {
                // block comment
                int end = sql.indexOf("*/", i);
                if (end == -1) {
                    // missing comment end
                    break;
                }
                i = end + 1;
            } else if (c == '#' || (c == '-' && lookahead(chars, i) == '-')) {
                // line comment
                i = skip(chars, i + 1, '\n', false);
            } else if (c == '?') {
                // question mark
                segments.add(sql.substring(segmentStart, i));
                segmentStart = i + 1;
            }
        }
        if (segmentStart < chars.length) {
            segments.add(sql.substring(segmentStart));
        } else {
            // add empty segment in case question mark was last char of sql
            segments.add("");
        }
        return segments.toArray(new String[0]);
    }

    private static int skip(char[] chars, int from, char until, boolean escape) {
        for (int i = from; i < chars.length; i++) {
            char curr = chars[i];
            if (escape) {
                char next = lookahead(chars, i);
                if ((curr == '\\' && (next == '\\' || next == until)) || (curr == until && next == until)) {
                    // should skip:
                    // 1) double \\ (backslash escaped with backslash)
                    // 2) \[until] ([until] char, escaped with backslash)
                    // 3) [until][until] ([until] char, escaped with [until])
                    i++;
                    continue;
                }
            }

            if (curr == until) {
                return i;
            }
        }
        return chars.length;
    }

    private static char lookahead(char[] chars, int pos) {
        pos = pos + 1;
        if (pos >= chars.length) {
            return '\0';
        }
        return chars[pos];
    }
}
