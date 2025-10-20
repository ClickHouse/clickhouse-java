package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.sql.SQLUtils;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.jdbc.internal.parser.antlr4.ClickHouseLexer;
import com.clickhouse.jdbc.internal.parser.antlr4.ClickHouseParser;
import com.clickhouse.jdbc.internal.parser.antlr4.ClickHouseParserBaseListener;
import com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlParser;
import com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlStatement;
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
            // TODO: set roles
            stmt.setInsert(parsedStmt.getStatementType() == StatementType.INSERT);
            stmt.setHasErrors(false);
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
            stmt.setHasErrors(false);
            stmt.setHasResultSet(isStmtWithResultSet(parsedStmt));
            stmt.setTable(parsedStmt.getTable());
            stmt.setInsertWithSelect(parsedStmt.containsKeyword("SELECT") && (parsedStmt.getStatementType() == StatementType.INSERT));

            Integer startIndex = parsedStmt.getPositions().get(ClickHouseSqlStatement.KEYWORD_VALUES_START);
            if (startIndex != null) {
                stmt.setAssignValuesGroups(1);
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

            stmt.setUseFunction(false);
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
                    parsedStatement.setTable(SQLUtils.unquoteIdentifier(ctx.tableIdentifier().getText()));
                }
            }

            @Override
            public void enterInsertStmt(ClickHouseParser.InsertStmtContext ctx) {
                ClickHouseParser.TableIdentifierContext tableId = ctx.tableIdentifier();
                if (tableId != null) {
                    parsedStatement.setTable(SQLUtils.unquoteIdentifier(tableId.getText()));
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
        ANTLR4
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
            }
            throw new SQLException("Unsupported parser: " + parserSelection);
        } catch (IllegalArgumentException e) {
            throw new SQLException("Unknown parser: " + name);
        }
    }
}
