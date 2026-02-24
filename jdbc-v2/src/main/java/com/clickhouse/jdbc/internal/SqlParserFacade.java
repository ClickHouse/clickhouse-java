package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.sql.SQLUtils;
import com.google.common.collect.ImmutableSet;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.jdbc.internal.parser.antlr4.ClickHouseLexer;
import com.clickhouse.jdbc.internal.parser.antlr4.ClickHouseParser;
import com.clickhouse.jdbc.internal.parser.antlr4.ClickHouseParserBaseListener;
import com.clickhouse.jdbc.internal.parser.antlr4_light.ClickHouseLightParser;
import com.clickhouse.jdbc.internal.parser.antlr4_light.ClickHouseLightParserBaseListener;
import com.clickhouse.jdbc.internal.parser.antlr4_light.ClickHouseLightParserListener;
import com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlParser;
import com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlStatement;
import com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlUtils;
import com.clickhouse.jdbc.internal.parser.javacc.JdbcParseHandler;
import com.clickhouse.jdbc.internal.parser.javacc.StatementType;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.IterativeParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public abstract class SqlParserFacade {

    private static final Logger LOG = LoggerFactory.getLogger(SqlParserFacade.class);

    public abstract ParsedStatement parsedStatement(String sql);

    public abstract ParsedPreparedStatement parsePreparedStatement(String sql);

    private static class JavaCCParser extends SqlParserFacade {

        @Override
        public ParsedStatement parsedStatement(String sql) {
            ParsedStatement stmt = new ParsedStatement();
            ClickHouseSqlStatement parsedStmt = parse(sql);
            if (parsedStmt.getStatementType() == StatementType.USE) {
                stmt.setUseDatabase(parsedStmt.getDatabase());
            }

            String rolesCount = parsedStmt.getSettings().get("_ROLES_COUNT");
            if (rolesCount != null) {
                int rolesCountInt = Integer.parseInt(rolesCount);
                ArrayList<String> roles = new ArrayList<>(rolesCountInt);
                boolean resetRoles = false;
                for (int i = 0; i < rolesCountInt; i++) {
                    String role = parsedStmt.getSettings().get("_ROLE_" + i);
                    if (role.equalsIgnoreCase("NONE")) {
                        resetRoles = true;
                    }
                    roles.add(parsedStmt.getSettings().get("_ROLE_" + i));
                }
                if (resetRoles) {
                    roles.clear();
                }
                stmt.setRoles(roles);
            }

            stmt.setInsert(parsedStmt.getStatementType() == StatementType.INSERT);
            stmt.setHasErrors(parsedStmt.getStatementType() == StatementType.UNKNOWN);
            stmt.setHasResultSet(isStmtWithResultSet(parsedStmt));
            return stmt;
        }

        private boolean isStmtWithResultSet(ClickHouseSqlStatement parsedStmt) {
            return parsedStmt.getStatementType() == StatementType.SELECT || parsedStmt.getStatementType() == StatementType.SHOW
                    || parsedStmt.getStatementType() == StatementType.EXPLAIN || parsedStmt.getStatementType() == StatementType.DESCRIBE
                    || parsedStmt.getStatementType() == StatementType.EXISTS || parsedStmt.getStatementType() == StatementType.CHECK;

        }

        @Override
        public ParsedPreparedStatement parsePreparedStatement(String sql) {
            ParsedPreparedStatement stmt = new ParsedPreparedStatement();
            ClickHouseSqlStatement parsedStmt = parse(sql);
            if (parsedStmt.getStatementType() == StatementType.USE) {
                stmt.setUseDatabase(parsedStmt.getDatabase());
            }
            stmt.setInsert(parsedStmt.getStatementType() == StatementType.INSERT);
            stmt.setHasErrors(parsedStmt.getStatementType() == StatementType.UNKNOWN);
            stmt.setHasResultSet(isStmtWithResultSet(parsedStmt));
            String tableName = parsedStmt.getTable();
            if (parsedStmt.getDatabase() != null && parsedStmt.getTable() != null) {
                tableName = String.format("%s.%s", parsedStmt.getDatabase(), parsedStmt.getTable());
            }
            stmt.setTable(tableName);
            stmt.setInsertWithSelect(parsedStmt.containsKeyword("SELECT") && (parsedStmt.getStatementType() == StatementType.INSERT));
            stmt.setAssignValuesGroups(parsedStmt.getValueGroups());

            Integer startIndex = parsedStmt.getPositions().get(ClickHouseSqlStatement.KEYWORD_VALUES_START);
            if (startIndex != null) {
                int endIndex = parsedStmt.getPositions().get(ClickHouseSqlStatement.KEYWORD_VALUES_END);
                stmt.setAssignValuesListStartPosition(startIndex);
                stmt.setAssignValuesListStopPosition(endIndex);
                String query = parsedStmt.getSQL();
                for (int i = startIndex + 1; i < endIndex; i++) {
                    char ch = query.charAt(i);
                    if (ch != '?' && ch != ',' && !Character.isWhitespace(ch)) {
                        stmt.setUseFunction(true);
                        break;
                    }
                }
            }

            stmt.setUseFunction(parsedStmt.isFuncUsed());
            parseParameters(sql, stmt);
            return stmt;
        }


        public ClickHouseSqlStatement parse(String sql) {
            JdbcParseHandler handler = JdbcParseHandler.getInstance();
            ClickHouseSqlStatement[] stmts = ClickHouseSqlParser.parse(sql, handler);
            if (stmts.length > 1) {
                throw new RuntimeException("More than one SQL statement found: " + sql);
            }
            return stmts[0];
        }
    }

    private static class ANTLR4Parser extends SqlParserFacade {

        @Override
        public ParsedStatement parsedStatement(String sql) {
            ParsedStatement stmt = new ParsedStatement();
            parseSQL(sql, new ParsedStatementListener(stmt));
            return stmt;
        }

        @Override
        public ParsedPreparedStatement parsePreparedStatement(String sql) {
            ParsedPreparedStatement stmt = new ParsedPreparedStatement();
            parseSQL(sql, new ParsedPreparedStatementListener(stmt));
            
            // Combine database and table like JavaCC does
            String tableName = stmt.getTable();
            if (stmt.getDatabase() != null && stmt.getTable() != null) {
                tableName = String.format("%s.%s", stmt.getDatabase(), stmt.getTable());
            }
            stmt.setTable(tableName);
            
            parseParameters(sql, stmt);
            return stmt;
        }

        protected ClickHouseParser parseSQL(String sql, ClickHouseParserBaseListener listener) {
            CharStream charStream = CharStreams.fromString(sql);
            ClickHouseLexer lexer = new ClickHouseLexer(charStream);
            ClickHouseParser parser = new ClickHouseParser(new CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(new ParserErrorListener());

            ClickHouseParser.QueryStmtContext parseTree = parser.queryStmt();
            IterativeParseTreeWalker.DEFAULT.walk(listener, parseTree);

            return parser;
        }

        private static class ParserErrorListener extends BaseErrorListener {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                LOG.debug("SQL syntax error at line: " + line + ", pos: " + charPositionInLine + ", " + msg);
            }
        }

        static boolean isStmtWithResultSet(ClickHouseParser.QueryStmtContext stmtContext) {
            ClickHouseParser.QueryContext qCtx = stmtContext.query();
            return qCtx != null && (qCtx.selectStmt() != null || qCtx.selectUnionStmt() != null ||
                    qCtx.showStmt() != null || qCtx.explainStmt() != null || qCtx.describeStmt() != null ||
                    qCtx.existsStmt() != null || qCtx.checkStmt() != null);
        }

        private static class ParsedStatementListener extends ClickHouseParserBaseListener {

            private final ParsedStatement parsedStatement;

            public ParsedStatementListener(ParsedStatement parsedStatement) {
                this.parsedStatement = parsedStatement;
            }

            @Override
            public void visitErrorNode(ErrorNode node) {
                parsedStatement.setHasErrors(true);
            }

            @Override
            public void enterQueryStmt(ClickHouseParser.QueryStmtContext ctx) {
                if (isStmtWithResultSet(ctx)) {
                    parsedStatement.setHasResultSet(true);
                }
            }

            @Override
            public void enterUseStmt(ClickHouseParser.UseStmtContext ctx) {
                if (ctx.databaseIdentifier() != null) {
                    parsedStatement.setUseDatabase(SQLUtils.unquoteIdentifier(ctx.databaseIdentifier().getText()));
                }
            }

            @Override
            public void enterSetRoleStmt(ClickHouseParser.SetRoleStmtContext ctx) {
                if (ctx.NONE() != null) {
                    parsedStatement.setRoles(Collections.emptyList());
                } else {
                    List<String> roles = new ArrayList<>();
                    for (ClickHouseParser.IdentifierContext id : ctx.setRolesList().identifier()) {
                        roles.add(SQLUtils.unquoteIdentifier(id.getText()));
                    }
                    parsedStatement.setRoles(roles);
                }
            }
        }

        protected static class ParsedPreparedStatementListener extends ClickHouseParserBaseListener {

            protected final ParsedPreparedStatement parsedStatement;

            public ParsedPreparedStatementListener(ParsedPreparedStatement parsedStatement) {
                this.parsedStatement = parsedStatement;
            }

            @Override
            public void enterQueryStmt(ClickHouseParser.QueryStmtContext ctx) {
                if (isStmtWithResultSet(ctx)) {
                    parsedStatement.setHasResultSet(true);
                }
            }

            @Override
            public void enterUseStmt(ClickHouseParser.UseStmtContext ctx) {
                if (ctx.databaseIdentifier() != null) {
                    parsedStatement.setUseDatabase(SQLUtils.unquoteIdentifier(ctx.databaseIdentifier().getText()));
                }
            }

            @Override
            public void enterSetRoleStmt(ClickHouseParser.SetRoleStmtContext ctx) {
                if (ctx.NONE() != null) {
                    parsedStatement.setRoles(Collections.emptyList());
                } else {
                    List<String> roles = new ArrayList<>();
                    for (ClickHouseParser.IdentifierContext id : ctx.setRolesList().identifier()) {
                        roles.add(SQLUtils.unquoteIdentifier(id.getText()));
                    }
                    parsedStatement.setRoles(roles);
                }
            }

            @Override
            public void enterColumnExprPrecedence3(ClickHouseParser.ColumnExprPrecedence3Context ctx) {
                super.enterColumnExprPrecedence3(ctx);
            }

            @Override
            public void visitErrorNode(ErrorNode node) {
                parsedStatement.setHasErrors(true);
            }

            @Override
            public void enterInsertParameterFuncExpr(ClickHouseParser.InsertParameterFuncExprContext ctx) {
                parsedStatement.setUseFunction(true);
            }

            @Override
            public void enterAssignmentValuesList(ClickHouseParser.AssignmentValuesListContext ctx) {
                parsedStatement.setAssignValuesListStartPosition(ctx.getStart().getStartIndex());
                parsedStatement.setAssignValuesListStopPosition(ctx.getStop().getStopIndex());
            }

            @Override
            public void enterTableExprIdentifier(ClickHouseParser.TableExprIdentifierContext ctx) {
                if (ctx.tableIdentifier() != null) {
                    extractAndSetDatabaseAndTable(ctx.tableIdentifier());
                }
            }

            @Override
            public void enterInsertStmt(ClickHouseParser.InsertStmtContext ctx) {
                ClickHouseParser.TableIdentifierContext tableId = ctx.tableIdentifier();
                if (tableId != null) {
                    extractAndSetDatabaseAndTable(tableId);
                }

                ClickHouseParser.ColumnsClauseContext columns = ctx.columnsClause();
                if (columns != null) {
                    List<ClickHouseParser.NestedIdentifierContext> names = columns.nestedIdentifier();
                    String[] insertColumns = new String[names.size()];
                    for (int i = 0; i < names.size(); i++) {
                        insertColumns[i] = names.get(i).getText();
                    }
                    parsedStatement.setInsertColumns(insertColumns);
                }

                parsedStatement.setInsert(true);
            }

            /**
             * Extracts database and table from parse tree using grammar structure.
             * Grammar: tableIdentifier = (databaseIdentifier DOT)? identifier
             * The grammar itself defines what's database vs table!
             * 
             * Examples:
             *   table -> databaseIdentifier=null, identifier="table"
             *   db.table -> databaseIdentifier="db", identifier="table"  
             *   a.b.c -> databaseIdentifier="a.b", identifier="c"
             */
            private void extractAndSetDatabaseAndTable(ClickHouseParser.TableIdentifierContext tableId) {
                if (tableId == null) {
                    return;
                }
                
                // Table is always the standalone identifier (last part)
                if (tableId.identifier() != null) {
                    parsedStatement.setTable(ClickHouseSqlUtils.unescape(tableId.identifier().getText()));
                }
                
                // Database is the databaseIdentifier part (if present)
                if (tableId.databaseIdentifier() != null) {
                    String database = tableId.databaseIdentifier().identifier().stream()
                        .map(id -> ClickHouseSqlUtils.unescape(id.getText()))
                        .collect(Collectors.joining("."));
                    parsedStatement.setDatabase(database);
                }
            }

            @Override
            public void enterDataClauseSelect(ClickHouseParser.DataClauseSelectContext ctx) {
                parsedStatement.setInsertWithSelect(true);
            }

            @Override
            public void enterDataClauseValues(ClickHouseParser.DataClauseValuesContext ctx) {
                parsedStatement.setAssignValuesGroups(ctx.assignmentValues().size());
            }

            @Override
            public void exitInsertParameterFuncExpr(ClickHouseParser.InsertParameterFuncExprContext ctx) {
                parsedStatement.setUseFunction(true);
            }
        }
    }

    private static class ANTLR4AndParamsParser extends ANTLR4Parser {

        @Override
        public ParsedPreparedStatement parsePreparedStatement(String sql) {
            ParsedPreparedStatement stmt = new ParsedPreparedStatement();
            parseSQL(sql, new ParseStatementAndParamsListener(stmt));
            
            // Combine database and table like JavaCC does
            String tableName = stmt.getTable();
            if (stmt.getDatabase() != null && stmt.getTable() != null) {
                tableName = String.format("%s.%s", stmt.getDatabase(), stmt.getTable());
            }
            stmt.setTable(tableName);
            
            return stmt;
        }

        private static class ParseStatementAndParamsListener extends ParsedPreparedStatementListener {

            public ParseStatementAndParamsListener(ParsedPreparedStatement parsedStatement) {
                super(parsedStatement);
            }

            @Override
            public void enterColumnExprParam(ClickHouseParser.ColumnExprParamContext ctx) {
                parsedStatement.appendParameter(ctx.start.getStartIndex());
            }


            @Override
            public void enterCteUnboundColParam(ClickHouseParser.CteUnboundColParamContext ctx) {
                parsedStatement.appendParameter(ctx.start.getStartIndex());
            }

            @Override
            public void enterInsertParameter(ClickHouseParser.InsertParameterContext ctx) {
                parsedStatement.appendParameter(ctx.start.getStartIndex());
            }

            @Override
            public void enterFromClause(ClickHouseParser.FromClauseContext ctx) {
                if (ctx.JDBC_PARAM_PLACEHOLDER() != null) {
                    parsedStatement.appendParameter(ctx.JDBC_PARAM_PLACEHOLDER().getSymbol().getStartIndex());
                }
            }

            @Override
            public void enterViewParam(ClickHouseParser.ViewParamContext ctx) {
                if (ctx.JDBC_PARAM_PLACEHOLDER() != null) {
                    parsedStatement.appendParameter(ctx.JDBC_PARAM_PLACEHOLDER().getSymbol().getStartIndex());
                }
            }
        }
    }

    private static class ANTLR4LightParser extends SqlParserFacade {
        private static final ImmutableSet<String> VERBS_WITHOUT_RESULT_SET = ImmutableSet.<String>builder()
                .add("ALTER")
                .add("ATTACH")
                .add("BACKUP")
                .add("CREATE")
                .add("DELETE")
                .add("DETACH")
                .add("DROP")
                .add("EXCHANGE")
                .add("GRANT")
                .add("INSERT")
                .add("KILL")
                .add("MOVE")
                .add("OPTIMIZE")
                .add("RENAME")
                .add("REPLACE")
                .add("RESTORE")
                .add("REVOKE")
                .add("SET")
                .add("TRUNCATE")
                .add("UNDROP")
                .add("UPDATE")
                .add("USE")
                .build();

        private static boolean isStmtWithResultSetVerb(String verb) {
            return !VERBS_WITHOUT_RESULT_SET.contains(verb);
        }

        private static String normalizeVerb(String rawVerb) {
            return rawVerb == null ? null : rawVerb.toUpperCase(Locale.ROOT);
        }

        @Override
        public ParsedStatement parsedStatement(String sql) {
            ParsedStatement stmt = new ParsedStatement();
            parseSQL(sql, new ANTLR4LightParser.ParsedStatementListener(stmt));
            return stmt;
        }

        @Override
        public ParsedPreparedStatement parsePreparedStatement(String sql) {
            ParsedPreparedStatement stmt = new ParsedPreparedStatement();
            parseSQL(sql, new ANTLR4LightParser.ParsedPreparedStatementListener(stmt));

            // Combine database and table like JavaCC does
            String tableName = stmt.getTable();
            if (stmt.getDatabase() != null && stmt.getTable() != null) {
                tableName = String.format("%s.%s", stmt.getDatabase(), stmt.getTable());
            }
            stmt.setTable(tableName);

            if (stmt.isInsert()) {
                parseInsertDataClause(sql, stmt);
            }

            parseParameters(sql, stmt);
            return stmt;
        }

        private static void parseInsertDataClause(String sql, ParsedPreparedStatement stmt) {
            int len = sql.length();
            boolean seenInsert = false;
            int i = 0;
            while (i < len) {
                char ch = sql.charAt(i);
                if (ClickHouseUtils.isQuote(ch)) {
                    i = ClickHouseUtils.skipQuotedString(sql, i, len, ch);
                    continue;
                }
                if (i + 1 < len) {
                    char nextCh = sql.charAt(i + 1);
                    if ((ch == '-' && nextCh == '-') || ch == '#') {
                        i = ClickHouseUtils.skipSingleLineComment(sql, i + 2, len);
                        continue;
                    }
                    if (ch == '/' && nextCh == '*') {
                        i = ClickHouseUtils.skipMultiLineComment(sql, i + 2, len);
                        continue;
                    }
                }

                if (Character.isLetter(ch)) {
                    int start = i;
                    i++;
                    while (i < len && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_')) {
                        i++;
                    }

                    String token = sql.substring(start, i).toUpperCase(Locale.ROOT);
                    if (!seenInsert) {
                        if ("INSERT".equals(token)) {
                            seenInsert = true;
                        }
                        continue;
                    }

                    if ("VALUES".equals(token)) {
                        stmt.setAssignValuesGroups(countValuesGroups(sql, i));
                        return;
                    }

                    if ("SELECT".equals(token)) {
                        stmt.setInsertWithSelect(true);
                        return;
                    }
                    continue;
                }

                i++;
            }
        }

        private static int countValuesGroups(String sql, int startIdx) {
            int len = sql.length();
            int groups = 0;
            int depth = 0;
            int i = startIdx;
            while (i < len) {
                char ch = sql.charAt(i);
                if (ClickHouseUtils.isQuote(ch)) {
                    i = ClickHouseUtils.skipQuotedString(sql, i, len, ch);
                    continue;
                }
                if (i + 1 < len) {
                    char nextCh = sql.charAt(i + 1);
                    if ((ch == '-' && nextCh == '-') || ch == '#') {
                        i = ClickHouseUtils.skipSingleLineComment(sql, i + 2, len);
                        continue;
                    }
                    if (ch == '/' && nextCh == '*') {
                        i = ClickHouseUtils.skipMultiLineComment(sql, i + 2, len);
                        continue;
                    }
                }

                if (ch == ';' && depth == 0) {
                    break;
                }

                if (ch == '(') {
                    if (depth == 0) {
                        groups++;
                    }
                    depth++;
                } else if (ch == ')' && depth > 0) {
                    depth--;
                }

                i++;
            }

            return groups;
        }

        protected ClickHouseLightParser parseSQL(String sql, ClickHouseLightParserListener listener) {
            CharStream charStream = CharStreams.fromString(sql);
            ClickHouseLexer lexer = new ClickHouseLexer(charStream);
            ClickHouseLightParser parser = new ClickHouseLightParser(new CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(new ANTLR4Parser.ParserErrorListener());

            ClickHouseLightParser.QueryStmtContext parseTree = parser.queryStmt();
            IterativeParseTreeWalker.DEFAULT.walk(listener, parseTree);

            return parser;
        }

        private static class ParsedStatementListener extends ClickHouseLightParserBaseListener {
            private final ParsedStatement stmt;

            public ParsedStatementListener(ParsedStatement stmt) {
                this.stmt = stmt;
                this.stmt.setHasResultSet(true);
                this.stmt.setInsert(false);
            }

            @Override
            public void visitErrorNode(ErrorNode node) {
                stmt.setHasErrors(true);
            }

            @Override
            public void enterInsertQueryStmt(ClickHouseLightParser.InsertQueryStmtContext ctx) {
                stmt.setStatementVerb("INSERT");
                stmt.setInsert(true);
                stmt.setHasResultSet(false);
            }

            @Override
            public void enterSetQueryStmt(ClickHouseLightParser.SetQueryStmtContext ctx) {
                stmt.setStatementVerb("SET");
                stmt.setHasResultSet(false);
            }

            @Override
            public void enterUseQueryStmt(ClickHouseLightParser.UseQueryStmtContext ctx) {
                stmt.setStatementVerb("USE");
                stmt.setHasResultSet(false);
            }

            @Override
            public void enterUseStmt(ClickHouseLightParser.UseStmtContext ctx) {
                if (ctx.identifier() != null) {
                    stmt.setUseDatabase(SQLUtils.unquoteIdentifier(ctx.identifier().getText()));
                }
            }

            @Override
            public void enterStatementVerb(ClickHouseLightParser.StatementVerbContext ctx) {
                String verb = normalizeVerb(ctx.getText());
                stmt.setStatementVerb(verb);
                stmt.setHasResultSet(isStmtWithResultSetVerb(verb));
            }
        }

        private static class ParsedPreparedStatementListener extends ClickHouseLightParserBaseListener {

            private final ParsedPreparedStatement stmt;

            public ParsedPreparedStatementListener(ParsedPreparedStatement stmt) {
                this.stmt = stmt;
                this.stmt.setHasResultSet(true);
                this.stmt.setInsert(false);
            }

            @Override
            public void visitErrorNode(ErrorNode node) {
                stmt.setHasErrors(true);
            }

            @Override
            public void enterInsertQueryStmt(ClickHouseLightParser.InsertQueryStmtContext ctx) {
                stmt.setStatementVerb("INSERT");
                stmt.setInsert(true);
                stmt.setHasResultSet(false);
            }

            @Override
            public void enterSetQueryStmt(ClickHouseLightParser.SetQueryStmtContext ctx) {
                stmt.setStatementVerb("SET");
                stmt.setHasResultSet(false);
            }

            @Override
            public void enterUseQueryStmt(ClickHouseLightParser.UseQueryStmtContext ctx) {
                stmt.setStatementVerb("USE");
                stmt.setHasResultSet(false);
            }

            @Override
            public void enterUseStmt(ClickHouseLightParser.UseStmtContext ctx) {
                if (ctx.identifier() != null) {
                    stmt.setUseDatabase(SQLUtils.unquoteIdentifier(ctx.identifier().getText()));
                }
            }

            @Override
            public void enterStatementVerb(ClickHouseLightParser.StatementVerbContext ctx) {
                String verb = normalizeVerb(ctx.getText());
                stmt.setStatementVerb(verb);
                stmt.setHasResultSet(isStmtWithResultSetVerb(verb));
            }

            @Override
            public void enterInsertTableStmt(ClickHouseLightParser.InsertTableStmtContext ctx) {
                ClickHouseLightParser.TableIdentifierContext tableIdentifier = ctx.tableIdentifier();
                if (tableIdentifier == null) {
                    return;
                }

                List<ClickHouseLightParser.IdentifierContext> identifiers = tableIdentifier.identifier();
                if (identifiers.isEmpty()) {
                    return;
                }

                if (identifiers.size() == 1) {
                    stmt.setTable(ClickHouseSqlUtils.unescape(identifiers.get(0).getText()));
                } else {
                    stmt.setDatabase(identifiers.subList(0, identifiers.size() - 1).stream()
                            .map(id -> ClickHouseSqlUtils.unescape(id.getText()))
                            .collect(Collectors.joining(".")));
                    stmt.setTable(ClickHouseSqlUtils.unescape(identifiers.get(identifiers.size() - 1).getText()));
                }

                ClickHouseLightParser.ColumnsClauseContext columnsClause = ctx.columnsClause();
                if (columnsClause != null) {
                    List<ClickHouseLightParser.NestedIdentifierContext> columns = columnsClause.nestedIdentifier();
                    String[] insertColumns = new String[columns.size()];
                    for (int i = 0; i < columns.size(); i++) {
                        insertColumns[i] = columns.get(i).getText();
                    }
                    stmt.setInsertColumns(insertColumns);
                }
            }
        }
    }

    private static void parseParameters(String originalQuery, ParsedPreparedStatement stmt) {
        int len = originalQuery.length();
        for (int i = 0; i < len; i++) {
            char ch = originalQuery.charAt(i);
            if (ClickHouseUtils.isQuote(ch)) {
                i = ClickHouseUtils.skipQuotedString(originalQuery, i, len, ch) - 1;
            } else if (ch == '?') {
                int idx = ClickHouseUtils.skipContentsUntil(originalQuery, i + 2, len, '?', ':');
                if (idx < len && originalQuery.charAt(idx - 1) == ':' && originalQuery.charAt(idx) != ':'
                        && originalQuery.charAt(idx - 2) != ':') {
                    i = idx - 1;
                } else {
                    stmt.appendParameter(i);
                }
            } else if (ch == ';') {
                continue;
            } else if (i + 1 < len) {
                char nextCh = originalQuery.charAt(i + 1);
                if ((ch == '-' && nextCh == ch) || (ch == '#')) {
                    i = ClickHouseUtils.skipSingleLineComment(originalQuery, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = ClickHouseUtils.skipMultiLineComment(originalQuery, i + 2, len) - 1;
                }
            }
        }
    }


    public enum SQLParser {
        /**
         * JavaCC used to determine sql type (SELECT, INSERT, etc.) and extract some information
         * Separate procedure parses sql for `?` parameter placeholders.
         */
        JAVACC,

        /**
         * ANTLR4 used to determine sql type (SELECT, INSERT, etc.) and extract some information and parameters
         */
        ANTLR4_PARAMS_PARSER,

        /**
         * ANTLR4 used to determine sql type (SELECT, INSERT, etc.), extract some information.
         * Separate procedure parses sql for `?` parameter placeholders.
         */
        ANTLR4,

        /**
         * Lightweight parser that extracts only required information from statement.
         * There passes invalid statements to server. It is done to not block unknown statements from execution.
         *
         */
        ANTLR4_LIGHT,
    }

    public static SqlParserFacade getParser(String name) throws SQLException {
        try {
            SQLParser parserSelection = SQLParser.valueOf(name);
            switch (parserSelection) {
                case JAVACC:
                    return new JavaCCParser();
                case ANTLR4_PARAMS_PARSER:
                    return new ANTLR4AndParamsParser();
                case ANTLR4:
                    return new ANTLR4Parser();
                case ANTLR4_LIGHT:
                    return new ANTLR4LightParser();
            }
            throw new SQLException("Unsupported parser: " + parserSelection);
        } catch (IllegalArgumentException e) {
            throw new SQLException("Unknown parser: " + name);
        }
    }
}
