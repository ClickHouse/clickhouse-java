package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.sql.SQLUtils;
import com.clickhouse.jdbc.internal.parser.ClickHouseLexer;
import com.clickhouse.jdbc.internal.parser.ClickHouseParser;
import com.clickhouse.jdbc.internal.parser.ClickHouseParserBaseListener;
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

    static final class ANTLR4Parser extends SqlParserFacade {

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
            return stmt;
        }

        private ClickHouseParser parseSQL(String sql, ClickHouseParserBaseListener listener) {
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

        private static final class ParsedPreparedStatementListener extends ClickHouseParserBaseListener {

            private final ParsedPreparedStatement parsedStatement;

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
            public void enterColumnExprParam(ClickHouseParser.ColumnExprParamContext ctx) {
                parsedStatement.appendParameter(ctx.start.getStartIndex());
            }

            @Override
            public void enterColumnExprPrecedence3(ClickHouseParser.ColumnExprPrecedence3Context ctx) {
                super.enterColumnExprPrecedence3(ctx);
            }

            @Override
            public void enterCteUnboundColParam(ClickHouseParser.CteUnboundColParamContext ctx) {
                parsedStatement.appendParameter(ctx.start.getStartIndex());
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

    public enum SQLParser {
        /**
         * JavaCC used to determine sql type (SELECT, INSERT, etc.) and extract some information
         * Separate procedure parses sql for `?` parameter placeholders.
         */
        JAVACC_PARAMS_PARSER,

        /**
         * ANTLR4 used to determine sql type (SELECT, INSERT, etc.) and extract some information
         * Separate procedure parses sql for `?` parameter placeholders.
         */
        ANTLR4_PARAMS_PARSER,

        /**
         * ANTLR4 used to determine sql type (SELECT, INSERT, etc.), extract some information
         * and determine parameter positions.
         */
        ANTLR4
    }

    public static SqlParserFacade getParser(String name) throws SQLException {
        try {
            SQLParser parserSelection = SQLParser.valueOf(name);
            switch (parserSelection) {
                case JAVACC_PARAMS_PARSER:
                    return null;
                case ANTLR4_PARAMS_PARSER:
                    return null;
                case ANTLR4:
                    return new ANTLR4Parser();
            }
            throw new SQLException("Unsupported parser: " + parserSelection);
        } catch (IllegalArgumentException e) {
            throw new SQLException("Unknown parser: " + name);
        }
    }
}
